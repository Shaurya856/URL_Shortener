# Snip — URL Shortener + Analytics Platform

A URL shortener where the redirect path and the analytics path are deliberately
separate systems: redirects are cache-first and synchronous, click tracking is
asynchronous and decoupled via a replicated Kafka cluster. The point of the
project is that split, and the distributed-systems mechanics behind it
(partitioning, replication, consumer groups, coordinated ID allocation) —
not the shortening itself.

## Architecture

```
                         ┌─────────────────────────────────────────┐
                         │              READ PATH (hot)             │
                         │                                           │
  Browser ── GET /code ──┤  Django (3 replicas)                     │
                         │      │                                    │
                         │      ▼                                    │
                         │  Redis GET short:<code>  ──── hit ───►  302 redirect
                         │      │ miss                                (fast path,
                         │      ▼                                     no DB hit)
                         │  Postgres SELECT ─► Redis SET (cache-aside)│
                         │      │                                    │
                         │      ▼                                    │
                         │   302 redirect (sent immediately)         │
                         │      │                                    │
                         │      └──► Kafka producer.send() [async,   │
                         │           non-blocking, acks=all]         │
                         └───────────────────┬───────────────────────┘
                                              │
                         ┌────────────────────▼──────────────────────┐
                         │        KAFKA CLUSTER (KRaft, 3 brokers)    │
                         │                                             │
                         │   topic: click_events                      │
                         │   partitions: 3   replication-factor: 3    │
                         │   min.insync.replicas: 2                   │
                         │                                             │
                         │   broker 1 ─┐                               │
                         │   broker 2 ─┼─ controller quorum (Raft)     │
                         │   broker 3 ─┘   any single broker can die   │
                         │                 without losing a partition │
                         └───────────────────┬───────────────────────┘
                                              │
                         ┌────────────────────▼──────────────────────┐
                         │             WRITE PATH (cold)              │
                         │                                             │
                         │  consumer group: analytics-consumer        │
                         │  3 replicas, 1 partition each (steady state)│
                         │  `manage.py consume_clicks`                 │
                         │      │  batches every 100 msgs / 5s        │
                         │      ├──► Postgres: bulk_create(Click)     │
                         │      └──► Redis: INCR / HINCRBY / ZINCRBY  │
                         │           rolling aggregate counters       │
                         └─────────────────────┬───────────────────────┘
                                                │
                          Stats pages read totals/daily/referrers
                          straight from Redis (falling back to a
                          Postgres aggregate query if Redis is cold),
                          never from Kafka directly.
```

**Read path** (`GET /<code>`): Redis cache-aside lookup → Postgres on miss →
redirect fires immediately → click event published to Kafka *after* the
redirect is already on the wire. `producer.send()` returns a future we never
block on, so the durability settings below add zero latency to the request —
publish failures (or a fully-down cluster) are logged and swallowed. A dead
Kafka cluster degrades analytics, never redirects.

**Write path**: 3 consumer replicas in one consumer group (`analytics-consumer`)
read `click_events`, batch inserts into Postgres (`Click.objects.bulk_create`),
and update rolling Redis counters (`stats:total:<code>`, `stats:daily:<code>`,
`stats:referrers:<code>`) so stats pages are O(1) Redis reads instead of
scanning the `Click` table on every view.

Offsets are committed manually after a batch's Postgres/Redis writes succeed,
not before — this is **at-least-once** delivery: a crash mid-batch replays
those events on restart rather than skipping them. Replays are made safe by
`event_id`, a UUID assigned when the event is published (not when it's
consumed): the Postgres insert uses `ignore_conflicts=True` against a unique
constraint on `Click.event_id`, and the Redis counter increments are gated
behind a `SET NX` dedup key per `event_id` — so a replayed event lands
exactly once in both stores no matter how many times it's redelivered.

### Why Kafka here, and why 3 brokers / RF=3

Click volume can spike (a link goes viral) independently of write capacity
on the redirect path. Kafka absorbs that burst as a durable log — a consumer
can fall behind and catch up without redirects ever queuing behind a
database write. It also means the redirect service and the analytics
service can be deployed, scaled, and restarted independently.

`click_events` is created with **3 partitions / replication-factor 3 /
min.insync.replicas=2**:

- **Partitions (3)** are what actually let multiple consumer replicas do
  useful work in parallel — a topic is the unit of pub/sub, a partition is
  the unit of parallelism. 3 partitions ↔ 3 consumer replicas is a
  deliberate match: each replica in the `analytics-consumer` group owns
  exactly one partition at a time, so there's no idle consumer and no
  partition shared by two consumers (Kafka guarantees a partition is only
  ever read by one consumer per group).
- **Replication factor 3** means every partition's data lives on all 3
  brokers (one leader, two followers). Losing any single broker loses zero
  data and zero partition availability — a follower on a surviving broker
  is simply promoted to leader.
- **min.insync.replicas=2** combined with the producer's `acks=all` (below)
  is what actually backs that guarantee: a write isn't acknowledged as
  durable until 2 of the 3 replicas have it, so even in the worst case (the
  leader dies immediately after acking) the data survives on at least one
  more broker.
- Producer config is `acks="all"` (not the previous `acks=0`) — a
  deliberate tradeoff. `acks=0` never confirms delivery at all; `acks=all`
  gives an honest durability guarantee for a "the click was captured"
  claim. It costs nothing on the request path here because `send()` is
  still fire-and-forget — we never call `.get()` on the returned future.
  The real cost is a producer whose internal buffer takes slightly longer
  to free up per batch, bounded by `max_block_ms=500` if that buffer ever
  fills during an outage.

### Why Redis here

Two different jobs, same tool:

- **Cache-aside on `code → long_url`**: the redirect is the hottest, most
  latency-sensitive path in the system, and it doesn't need to touch
  Postgres on every request once a code is warm.
- **Rolling aggregate counters**: `INCR`/`HINCRBY`/`ZINCRBY` let the consumer
  update stats in O(1) per event instead of the stats page running
  `GROUP BY` queries over a growing `Click` table on every load.
- **Rate limiting**: a Redis sorted-set sliding window guards `/shorten`
  against abuse without adding a stateful dependency to the app tier itself.
  Keyed off `client_ip()`, which only trusts `X-Forwarded-For` when
  `TRUST_X_FORWARDED_FOR=true` — off by default since nothing in this stack
  puts a proxy in front of `web` to set/strip that header, so trusting it
  unconditionally would let a client spoof its way around the limit.

### Distributed ID generation: pre-allocated ranges

Short codes are base62-encoded integers, and those integers now come from
**pre-allocated range claiming** instead of a Postgres round trip per
request:

- A single counter row (`IdCounter.next_value`) lives in Postgres.
- Each Django process — each gunicorn worker, in practice — claims a block
  of 100,000 IDs from it in **one atomic statement**:
  `UPDATE shortener_idcounter SET next_value = next_value + 100000
  WHERE id = 1 RETURNING next_value`. Postgres takes a row lock for the
  duration of that UPDATE, so two processes claiming at the same instant
  are serialized by the database and walk away with disjoint ranges — no
  `SELECT FOR UPDATE` needed, because there's nothing to read before
  deciding what to write.
- The process then hands out IDs from its local range in memory
  (`shortener/id_allocator.py`), guarded by a thread lock. **Zero DB round
  trips per shorten request** for ID generation itself — only one round
  trip per 100,000 codes.
- When a process's local range is exhausted, it transparently claims the
  next block. Same code path, no special-casing.
- **Accepted tradeoff**: on restart (or crash), any unused IDs left in a
  process's current block are simply never issued — worst case, 100,000
  codes burned per restart. Base62 alone covers 62⁴ ≈ 14.8M values for a
  4-character code, growing to 5+ characters automatically as the counter
  climbs, so losing a block on restart is cheap relative to the address
  space and not worth reclaiming.

**Alternative not implemented: Snowflake-style IDs.** A Snowflake ID embeds
a worker ID, a timestamp, and a per-millisecond sequence directly into the
generated ID, so every process can generate IDs completely independently —
zero coordination with any central store, ever, not even at block-exhaustion
time. It's the better choice at higher scale or when a Postgres round trip
of any frequency is unacceptable. The tradeoffs against pre-allocated
ranges: assigning and tracking unique worker IDs is itself a coordination
problem (often solved with Zookeeper/etcd or manual assignment), generated
IDs are longer and don't compress as tightly as small monotonic integers
through base62, and clock-drift/clock-skew across machines has to be
handled carefully since the timestamp is load-bearing. Pre-allocated ranges
were the right call here: far simpler to implement and reason about, and a
DB round trip once per 100,000 requests is not a bottleneck at this scale.

## Stack

Django (server-rendered templates, no frontend build step) · PostgreSQL ·
Redis · Kafka (3-broker KRaft cluster, no Zookeeper) · Tailwind CSS (CDN) ·
Chart.js (CDN) · Docker Compose · Kubernetes manifests.

## Running locally (Docker Compose — primary path)

```bash
cp .env.example .env
docker compose up -d --build --scale consumer=3
```

Then open **http://localhost:8000**.

This brings up: `postgres`, `redis`, a 3-broker Kafka cluster
(`kafka-1`/`kafka-2`/`kafka-3`, KRaft combined controller+broker mode),
`kafka-init` (one-shot: creates `click_events` with partitions=3,
replication-factor=3, then exits), `web` (Django + gunicorn, runs
migrations on boot), and 3 replicas of `consumer` (the Kafka consumer, same
image as `web`, entrypoint override: `python manage.py consume_clicks`).

`--scale consumer=3` is what matches the consumer group's replica count to
`click_events`'s partition count — without it, Compose starts a single
`consumer` container, which still works fine (one consumer just owns all 3
partitions), you just don't get the "multiple replicas splitting the
partitions" story for a demo.

Useful commands:

```bash
docker compose logs -f web consumer                 # watch the redirect + analytics paths
docker compose exec kafka-1 kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic click_events
docker compose exec kafka-1 kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 --describe --group analytics-consumer
docker compose down                                  # stop everything
docker compose down -v                                # stop and wipe volumes (fresh DB/Kafka)
```

> Notes:
> - `bitnami/kafka` moved its actively-maintained tags behind Bitnami
>   Secure Images in 2025; `docker-compose.yml` pins `bitnamilegacy/kafka:3.7`,
>   Bitnami's frozen free snapshot of that tag.
> - All 3 brokers are pinned to the same `KAFKA_KRAFT_CLUSTER_ID` in
>   `docker-compose.yml`. Without that, each container generates its own
>   random cluster ID on first boot and the controller quorum never forms —
>   each node ends up bootstrapping its own independent single-node
>   "cluster" that can't see the other two.

## Failure-survival demo

`scripts/failure_demo.sh` is written for live/screen-shared demos: it sends
real traffic through the running stack, kills a Kafka broker mid-flow, kills
a consumer replica mid-flow, and proves no click was lost — using
`kafka-consumer-groups.sh --describe` output as the evidence, not just "it
still returned 200".

```bash
docker compose up -d --build --scale consumer=3
./scripts/failure_demo.sh
```

What it does, in order:

1. Shortens and clicks a small batch of links, prints the consumer group's
   per-partition offsets as a baseline.
2. `docker compose stop kafka-2` — stops one of the three brokers. Sends
   more shorten + redirect traffic and shows it still succeeds: the
   producer's bootstrap list includes all 3 brokers, so it fails over to
   whichever partition leaders are still alive. `kafka-topics.sh --describe`
   shows the in-sync-replica set shrink from `{1,2,3}` to `{1,3}` and any
   partition kafka-2 led get a new leader — the topic never loses
   availability. Restarts kafka-2 and shows the ISR heal back to `{1,2,3}`.
3. Stops one running `consumer` replica. Sends more traffic, then shows
   `kafka-consumer-groups.sh --describe` — the dead replica's partition is
   picked up by one of the two survivors (visible as a different consumer
   ID/host owning it), lag stays at 0. Restarts the replica and shows the
   group rebalance again to spread back across all 3.
4. Calls `/api/stats/<code>` for every link created during the whole run
   and confirms every single one shows `total_clicks=1` — nothing published
   during either failure was lost.

Everything the script stops, it restarts before exiting, so the stack is
left healthy (3/3 brokers, 3/3 consumers) either way.

## Running locally without Docker

Requires a local Postgres, Redis, and Kafka (or point `POSTGRES_HOST` /
`REDIS_HOST` / `KAFKA_BOOTSTRAP_SERVERS` at remote ones).

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # edit hosts to localhost
python manage.py migrate
python manage.py runserver
# in a second terminal, same venv:
python manage.py consume_clicks
```

## How it works, end to end

1. **Home page** (`/`) — paste a URL, get a short link with a copy button.
2. **`POST /shorten`** — rate-limited per IP (Redis sliding window, default
   10 requests / 60s), generates a base62 code from an ID claimed out of
   this process's pre-allocated range (see "Distributed ID generation"
   above — no DB round trip on the common path), creates the `ShortURL`
   row, tags it to the current session for dashboard ownership.
3. **`GET /<code>`** — cache-aside redirect (see architecture above), then
   publishes a click event to the Kafka cluster non-blocking.
4. **Consumer group** (3 replicas) — batches `click_events` into `Click`
   rows and Redis aggregate counters, one partition per replica.
5. **`/dashboard`** — every link created in the current browser session,
   with live click counts pulled from Redis.
6. **`/stats/<code>`** — total clicks, a clicks-per-day bar chart
   (Chart.js), and top referrers, all read from Redis aggregates (falls
   back to a Postgres `GROUP BY` if Redis is cold, e.g. right after a
   deploy).
7. **`/api/stats/<code>`** — same data as the stats page, as JSON.

## Data model

- **ShortURL**: `code` (unique, base62), `long_url`, `owner` (nullable FK to
  `User` — no auth flow is wired up, but the field exists for one),
  `session_key` (session-based ownership for the dashboard), `created_at`,
  `expires_at` (nullable).
- **Click**: `short_url` (FK), `event_id` (UUID, unique — dedup key for
  at-least-once Kafka redelivery), `timestamp`, `referrer`, `ip_hash`
  (SHA-256 salted with `IP_HASH_SALT`, raw IPs are never stored), `user_agent`.
- **IdCounter**: single-row central counter backing pre-allocated ID range
  claims (see above) — not exposed anywhere in the app itself.

## Kubernetes (secondary — "how it'd deploy")

`k8s/` has hand-written manifests for the two things this project actually
owns — the Django app and the analytics consumer — and assumes Postgres,
Redis, and Kafka are installed via their existing Bitnami Helm charts
rather than hand-rolled. The Kafka install uses the chart's own multi-node
StatefulSet (`controller.replicaCount=3`, KRaft combined mode) rather than
a hand-rolled one, consistent with "use the existing chart, don't
reinvent stateful infra ourselves":

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
kubectl apply -f k8s/namespace.yaml

helm install postgres bitnami/postgresql -n url-shortener \
  --set auth.database=urlshortener --set auth.username=urlshortener
helm install redis bitnami/redis -n url-shortener --set architecture=standalone
helm install kafka bitnami/kafka -n url-shortener \
  --set kraft.enabled=true \
  --set controller.replicaCount=3 \
  --set broker.replicaCount=0

cp k8s/secret.example.yaml k8s/secret.yaml   # fill in real values, don't commit it
kubectl apply -f k8s/configmap.yaml -f k8s/secret.yaml
kubectl apply -f k8s/migrate-job.yaml
kubectl apply -f k8s/kafka-topic-init-job.yaml
kubectl apply -f k8s/web-deployment.yaml -f k8s/web-service.yaml
kubectl apply -f k8s/consumer-deployment.yaml
```

- **`web`**: 3 replicas — the horizontal-scaling story for the redirect
  service, since it's stateless behind Redis/Postgres. Each pod claims and
  hands out its own pre-allocated ID range independently.
- **`consumer`**: 3 replicas, matching `click_events`'s 3 partitions — same
  reasoning as the docker-compose `--scale consumer=3` above.
- **`migrate-job.yaml`**: a one-shot `Job` so schema migrations run once
  per deploy instead of racing across 3 rolling web pods.
- **`kafka-topic-init-job.yaml`**: a one-shot `Job` mirroring
  docker-compose's `kafka-init` service — creates `click_events` with the
  same partitions/replication-factor/min-ISR against the Helm-installed
  cluster.

**Not implemented, by design**: Horizontal Pod Autoscaling. The `web`
Deployment is a good HPA candidate (stateless, CPU-bound on redirects) —
future work, not done here to keep scope to a weekend.

## What would make this production-grade

Honestly acknowledged, not implemented in this pass:

- **Redis is a single instance.** It's on the hot path for every redirect
  (cache-aside) and every stats read (aggregate counters), so it's
  currently a single point of failure — losing it doesn't lose data
  (Postgres is the source of truth, Redis is a cache/derived-state store)
  but it does mean every redirect falls through to Postgres and every
  stats page falls through to a `GROUP BY` until Redis comes back. Next
  step: Redis Sentinel for HA failover, or Redis Cluster if the
  rate-limiter/cache working set outgrows one node.
- **Postgres is a single instance.** It's the source of truth for
  `ShortURL`, `Click`, and the ID counter — losing it is real data loss
  risk, not just a latency blip. Next step: a primary + read replica setup
  (stats/dashboard reads could go to a replica), managed failover
  (Patroni, or a managed Postgres service), and WAL-based backups.
- **Kafka's controller quorum tolerates 1 broker loss, not more.** With 3
  controllers, the Raft quorum needs 2 of 3 to make progress — losing 2
  brokers simultaneously stalls metadata changes (though already-replicated
  partition data isn't lost). 5 controllers would tolerate 2 simultaneous
  losses; not done here to keep the demo footprint small.
- No HPA (noted above), no TLS between services, no auth on the Kafka
  cluster or Redis (fine for a local/demo network, not for a shared one).
- **`DEBUG` and `ALLOWED_HOSTS` default to dev-friendly values** (`true` /
  `*`) so the app runs out of the box locally — `DJANGO_DEBUG=false` and a
  real `DJANGO_ALLOWED_HOSTS` value must be set via env vars before this
  is exposed anywhere outside a local/demo network.

## What's explicitly out of scope

Custom domains, full OAuth/signup auth (session-based ownership only), QR
codes, real geo-IP lookups, K8s autoscaling.

## Screenshots

_placeholder — add screenshots of the home page, dashboard, and stats page
here before sharing._

## Project structure

```
config/                 Django project (settings, urls, wsgi)
shortener/               app: models, views, redis client, kafka producer
  id_allocator.py        pre-allocated range ID claiming
  management/commands/  consume_clicks.py — the analytics consumer entrypoint
  templates/shortener/  home / dashboard / stats pages (Tailwind + Chart.js CDN)
  migrations/
docker/entrypoint.sh    waits for Postgres, runs migrations, execs the CMD
scripts/failure_demo.sh interview demo: kills a broker + a consumer, proves no data loss
k8s/                     Deployment/Service/Job manifests for web + consumer + init jobs
docker-compose.yml       full local stack: postgres, redis, 3-broker kafka, web, consumer×N
```

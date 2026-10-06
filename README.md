# QueueLess

Smart virtual and physical queueing for businesses such as clinics, barbers, restaurants and government offices.
Customers take a ticket (virtually, or by scanning a QR code on site), track their position and estimated wait,
and get notified when their turn approaches. Businesses create queues, call the next customer, and mark them served or skipped.

QueueLess is also a hands-on learning project: a real product used to learn **microservices, FastAPI and Kafka**
step by step, one concept at a time.

> **Status:** early development. Two services run in Docker: the **Queue Service** publishes domain events to Kafka, and the **Notification Service** consumes them. More consumers come next (see Roadmap).

## Architecture

Target architecture (the destination, not where we are today):

```
Customer / Business
        |
   API Gateway
        |
 +------+---------------+
 |      |               |
Identity  Queue        Business
Service   Service      Service
 |      |               |
 +------+-------+-------+
                |
              Kafka
                |
   +------------+-------------+
   |            |             |
Notification  Waiting-Time  Analytics
 Service       Service       Service
```

Principles: database per service, no shared business logic between services, events represent facts,
idempotent consumers, and failure is normal. Kafka carries domain events (for example `CustomerJoinedQueue`);
queries such as "what is my position?" stay synchronous over HTTP.

## What exists today

```
queue-service ──(CustomerJoinedQueue, CustomerCalled, ...)──▶ Kafka: queue.events ──▶ notification-worker ──▶ notifications DB ◀── notification-api
      │                                                                              (consumer group                         (GET /users/{id}/notifications)
      ▼                                                                               "notification-service")
  queueless DB
```

Each service has **its own database** (database per service). They never read each other's tables; they communicate only through Kafka events.

### Queue Service

`services/queue_service`: a FastAPI service backed by PostgreSQL.

- **Stack:** FastAPI, SQLAlchemy 2.x, Alembic, Pydantic v2 / pydantic-settings, PostgreSQL 17, Apache Kafka 4.1 (KRaft) with confluent-kafka, pytest, Docker / Docker Compose
- **Domain:** `Business` 1-N `Queue` 1-N `QueueEntry`
- **Queue entry states:** `WAITING(1)`, `CALLED(2)`, `SERVED(3)`, `LEFT(4)`, `NO_SHOW(5)`
- **Concurrency:** ticket numbers and "call next" run under a row lock (`SELECT ... FOR UPDATE`) on the queue row, so simultaneous requests are serialized
- **State machine:** serve, no-show and leave are atomic compare-and-set updates (`UPDATE ... WHERE status IN (allowed) RETURNING`). If two actions race on one ticket, exactly one wins and the other gets `409`.
- **Rules enforced in the database:** unique ticket number per queue; one active (WAITING/CALLED) entry per user per queue (a partial unique index, so a user can rejoin after leaving or being served)

### Ticket lifecycle

```
            call-next             serve
 WAITING ─────────────> CALLED ─────────> SERVED
    │                      │
    │ leave                │ no-show
    v                      v
  LEFT                  NO_SHOW
```

`SERVED`, `LEFT` and `NO_SHOW` are final. Any other move returns `409`, for example `Cannot change entry from WAITING to SERVED`.
The allowed moves are defined in one place, `ALLOWED_TRANSITIONS` in `app/services/queue_service.py`.

### Events (Kafka)

Every successful state change publishes one event to the topic **`queue.events`** (3 partitions), **after** the database commit.

| Action | Event type |
| ------ | ---------- |
| join | `CustomerJoinedQueue` |
| call-next | `CustomerCalled` |
| serve | `CustomerServed` |
| no-show | `CustomerMarkedNoShow` |
| leave | `CustomerLeftQueue` |

- **Message key = `queue_id`.** All events of one queue go to the same partition, so consumers see them in order (joined → called → served). Different queues spread across partitions and are processed in parallel.
- **One topic, many event types.** Kafka orders messages only within a partition, so events that must stay in order relative to each other (the same queue's) share a topic. The type is in the payload and in an `event_type` header.
- **Envelope** (`app/events/envelope.py`):

```json
{
  "event_id": "f898d99e-3505-4297-bb4c-e6197a0cb26f",
  "event_type": "CustomerCalled",
  "event_version": 1,
  "occurred_at": "2026-10-03T09:02:00Z",
  "producer": "queue-service",
  "data": {"entry_id": 12, "queue_id": 1, "user_id": 42, "ticket_number": 43, "status": "CALLED",
           "joined_at": "2026-10-03T08:40:00Z", "called_at": "2026-10-03T09:02:00Z", "served_at": null}
}
```

`event_id` lets consumers deduplicate, and `event_version` lets the schema evolve.

- **Delivery reports:** a background thread calls `producer.poll()` continuously, so delivery success or failure is logged within ~0.5 s. Without it, callbacks only run when the *next* event is published.
- **Producer settings:** `acks=all`, `enable.idempotence=true` (explicit, because librdkafka defaults it to false), and `partitioner=murmur2_random` (the same key → partition mapping as Java clients). One producer per process; buffered messages are flushed on shutdown.
- **Known limitation (dual write):** the database commit and the Kafka publish are two separate writes. If Kafka is unreachable, the API still succeeds and the event is lost after `message.timeout.ms`, with only an error in the log. This will be fixed by the **outbox pattern** (see Roadmap).

### Notification Service

`services/notification_service`: one image, run as three containers:

| Container | Command | Role |
| --------- | ------- | ---- |
| `notification-migrate` | `alembic upgrade head` | one-shot migration job |
| `notification-worker` | `python -m app.worker` | Kafka consumer: turns events into notifications (scalable) |
| `notification-api` | `uvicorn app.main:app` | `GET /users/{user_id}/notifications` on http://localhost:8001 |

Events handled: `CustomerJoinedQueue` → "You joined the queue…", `CustomerCalled` → "It's your turn!…". All other event types are ignored, but still committed.

**Consumer design:**

- **Consumer group** `notification-service`: all worker instances share the 3 partitions of `queue.events`. Run more with `docker compose up -d --scale notification-worker=2`.
- **`enable.auto.commit=false`**: the offset is committed only **after** the notification is saved (at-least-once).
- **Idempotent handler**: `notifications.event_id` is UNIQUE. If Kafka delivers the same event twice (crash before commit, rebalance, manual replay), the second insert fails on the constraint and is ignored, so exactly one notification is stored per event.
- **`auto.offset.reset=earliest`**: a brand-new group starts from the oldest retained event, so nothing produced before the first deploy is missed.
- **`partition.assignment.strategy=cooperative-sticky`**: incremental rebalancing. When a worker joins or leaves, only the partitions that must move are paused; the others keep working.
- **Error handling**: an unparsable message (*poison message*) is logged and skipped. A database outage does **not** commit; the worker `seek()`s back to the same offset and retries every 5 s.
- **Graceful shutdown**: on SIGTERM the worker finishes the current message and calls `consumer.close()`, so its partitions move to other workers immediately.
- **Contract, not code**: the worker validates events with its own Pydantic model (`app/events.py`) and does not import the queue service's code. Unknown extra fields are accepted, so the producer can add fields without breaking it.

### API

| Method | Path | Description |
| ------ | ---- | ----------- |
| GET | `/health` | Health check |
| POST | `/businesses` | Create a business |
| POST | `/businesses/{business_id}/queues` | Create a queue (starts OPEN) |
| POST | `/queues/{queue_id}/join` | Join a queue; body `{"user_id": 1}` (temporary until auth exists) |
| POST | `/queues/{queue_id}/call-next` | Call the lowest waiting ticket (`409` if nobody is waiting) |
| POST | `/queues/{queue_id}/entries/{entry_id}/serve` | CALLED → SERVED (sets `served_at`) |
| POST | `/queues/{queue_id}/entries/{entry_id}/no-show` | CALLED → NO_SHOW |
| POST | `/queues/{queue_id}/entries/{entry_id}/leave` | WAITING → LEFT; body `{"user_id": 1}`, only the ticket's owner (otherwise `404`) |

Error conventions: `404` for an unknown business, queue or entry (also for an entry in another queue, or one belonging to another user), and `409` for a business-rule conflict (queue not open, already in the queue, invalid state change).

Interactive docs are served at `/docs` when the app is running.

## Getting started

Prerequisites: Docker. For local development you also need Python 3.12+.

### Option A: run everything in Docker

```bash
docker compose up --build
```

Containers, in start order:

| Service | What it does |
| ------- | ------------ |
| `postgres` | PostgreSQL 17, published on host port **5433** |
| `kafka` | Single-node Kafka 4.1 in KRaft mode (no ZooKeeper), published on host port **9094** |
| `kafka-init` | One-shot job: creates topic `queue.events` (3 partitions), then exits |
| `queue-migrate` | One-shot job: runs `alembic upgrade head`, then exits |
| `queue-service` | The API on http://localhost:8000. It starts only after postgres is healthy **and** both one-shot jobs have succeeded |
| `notification-postgres` | the Notification Service's own PostgreSQL 17, host port **5434** |
| `notification-migrate` | One-shot job: Notification Service migrations |
| `notification-worker` | Kafka consumer (no HTTP port; scale with `--scale notification-worker=N`) |
| `notification-api` | Notification read API on http://localhost:8001 |
| `kafka-ui` | Optional web UI on http://localhost:8080, only with `docker compose --profile tools up` |

Open http://localhost:8000/docs. Useful commands:

```bash
docker compose ps                       # queue-service should show "(healthy)"
docker compose logs -f queue-service
docker compose logs queue-migrate       # migration output
docker compose down                     # stop (keeps the data volume)
docker compose down -v                  # stop and wipe the databases
docker compose logs -f notification-worker              # see notifications being created
docker compose up -d --scale notification-worker=2      # watch a rebalance in the logs
curl localhost:8001/users/11/notifications
```

Watch events live (key, partition and headers included):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic queue.events --from-beginning \
  --property print.key=true --property print.partition=true --property print.headers=true

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic queue.events
```

### Development: live code reload inside Docker

`docker-compose.override.yml` is loaded **automatically** by `docker compose up`, on top of `docker-compose.yml`.
It is for development only:

- **Bind mounts** put your local `app/` folders (and `migrations/`) into the containers, so they run the code on your disk, not the copy baked into the image.
- **Auto-restart:** the APIs run `uvicorn --reload`, and the worker runs under `watchfiles`, which restarts it (with a graceful SIGINT) when a `.py` file changes.

| You changed… | What to do |
| ------------ | ---------- |
| Python code in `app/` | nothing: the process reloads within ~1 s (`docker compose logs -f <service>`) |
| a new Alembic migration | `docker compose run --rm queue-migrate` (or `notification-migrate`) |
| `docker-compose*.yml` or environment variables | `docker compose up -d` (recreates only the changed containers) |
| `requirements.txt` or `Dockerfile` | `docker compose up -d --build` (the image must be rebuilt) |

Run the production-like setup (no mounts, no reload) with `docker compose -f docker-compose.yml up -d`.
On Docker Desktop (macOS/Windows), if changes are not detected, add `WATCHFILES_FORCE_POLLING: "true"` to the service's `environment`.

### Option B: run the service locally (fast reload while coding)

```bash
# 1. Start both databases, Kafka and the topic job
docker compose up -d postgres notification-postgres kafka kafka-init

# 2. Create a virtualenv and install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r services/queue_service/requirements-dev.txt -r services/notification_service/requirements-dev.txt

# 3. Configure the environment: create .env in the repo root
echo 'DATABASE_URL=postgresql+psycopg://queueless:queueless@localhost:5433/queueless' > .env
echo 'NOTIFICATION_DATABASE_URL=postgresql+psycopg://notification:notification@localhost:5434/notifications' >> .env

# 4. Apply migrations and run the service
cd services/queue_service
alembic upgrade head
uvicorn app.main:app --reload

# 5. In other terminals: the Notification Service (migrate, worker, API)
cd services/notification_service
alembic upgrade head
python -m app.worker
uvicorn app.main:app --reload --port 8001
```

Don't run options A and B at the same time: they use the same ports.

### Configuration

Settings are read from **environment variables**. `.env` is only a local-development convenience: `config.py` walks up from the
service directory and loads the first `.env` it finds. Both services share the root `.env`, so each `Settings` class uses
`extra="ignore"` to skip the other service's variables. The Docker image never contains `.env` (see `.dockerignore`), and Compose
passes `DATABASE_URL` directly.

| Variable | Local (Option B) | Inside Docker (Option A) |
| -------- | ---------------- | ------------------------ |
| `DATABASE_URL` | `...@localhost:5433/queueless` | `...@postgres:5432/queueless` |
| `NOTIFICATION_DATABASE_URL` | `...@localhost:5434/notifications` | `...@notification-postgres:5432/notifications` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` (default) | `kafka:9092` |
| `CONSUMER_GROUP_ID` | `notification-service` (default) | `notification-service` (default) |
| `QUEUE_EVENTS_TOPIC` | `queue.events` (default) | `queue.events` (default) |

Inside the Compose network, containers reach each other by **service name** on the **container** port (`postgres:5432`, `kafka:9092`).
Kafka needs two listeners for this because it tells clients which address to reconnect to (the *advertised* listener):
containers are told `kafka:9092`, and your machine is told `localhost:9094`.
`localhost:5433` only works from your machine, because `localhost` inside a container means that container itself.

### Tests

```bash
cd services/queue_service
python -m pytest -q
```

Test dependencies (`pytest`, `httpx`) live in `requirements-dev.txt` and are not installed in the Docker image.

Tests run against in-memory SQLite. SQLite ignores row locks, so they verify logic but not concurrency;
locking behaviour is checked manually against PostgreSQL. Partial indexes declare both `postgresql_where` and
`sqlite_where`, so the same rule holds in tests and in production.

| File | Covers |
| ---- | ------ |
| `tests/test_queue_flow.py` | join, duplicate join, 404s, call-next order, empty queue, called user can't rejoin |
| `tests/test_entry_transitions.py` | serve, no-show, leave + rejoin, final states, invalid moves, ownership, cross-queue 404 |
| `tests/test_events.py` | each action publishes the right event, key = queue id, order of a full lifecycle, failed actions publish nothing, unique event ids |

| `services/notification_service/tests/test_handlers.py` | event → notification, duplicate event stored once, ignored event types, tolerant to new fields, invalid message rejected, read API |

Run each service's tests from its own directory. Tests never touch Kafka: `conftest.py` overrides the `get_publisher` dependency with a `FakePublisher` that records events in a list.

### Manual concurrency checks (PostgreSQL)

```bash
# Two employees press "call next" at once: expect two different tickets
curl -s -X POST localhost:8000/queues/1/call-next & curl -s -X POST localhost:8000/queues/1/call-next & wait

# Serve and no-show race on one CALLED ticket: expect one 200 and one 409
curl -s -X POST localhost:8000/queues/1/entries/1/serve & curl -s -X POST localhost:8000/queues/1/entries/1/no-show & wait
```

### Migrations

In Docker, migrations run automatically through the `queue-migrate` job. Locally:

```bash
cd services/queue_service
alembic revision --autogenerate -m "describe the change"   # always review the generated file
alembic upgrade head
```

## Learning notes

QueueLess is a learning project, so the concepts behind each step are written down:

- [Kafka fundamentals](docs/kafka-fundamentals.md): log vs queue, KRaft architecture, brokers, partitions, segments, replication and ISR, topic configs and compaction, bootstrap servers and advertised listeners, producer internals, consumer groups, Kafka vs RabbitMQ, delivery guarantees, and a hands-on lab against this stack.

## Project structure

```
queueless/
├── docker-compose.yml          # databases, kafka, one-shot jobs, both services
├── docker-compose.override.yml # dev only: code mounts + auto-reload (loaded automatically)
├── docs/                       # learning notes (kafka-fundamentals.md, ...)
├── services/
│   ├── queue_service/
│   │   ├── Dockerfile
│   │   ├── .dockerignore
│   │   ├── requirements.txt        # runtime dependencies (go into the image)
│   │   ├── requirements-dev.txt    # + test tools
│   │   ├── alembic.ini
│   │   ├── app/
│   │   │   ├── api/routes/     # HTTP routes
│   │   │   ├── events/         # event envelope + Kafka publisher
│   │   │   ├── models/         # SQLAlchemy models
│   │   │   ├── schemas/        # Pydantic request/response schemas
│   │   │   ├── services/       # business logic
│   │   │   ├── config.py
│   │   │   ├── database.py
│   │   │   └── main.py
│   │   ├── migrations/         # Alembic
│   │   ├── tests/
│   │   └── pytest.ini
│   └── notification_service/
│       ├── Dockerfile              # no HEALTHCHECK: the image runs as worker AND api
│       ├── requirements.txt
│       ├── requirements-dev.txt
│       ├── alembic.ini
│       ├── app/
│       │   ├── config.py
│       │   ├── database.py
│       │   ├── models.py           # Notification (UNIQUE event_id)
│       │   ├── events.py           # consumer-side event contract (Pydantic)
│       │   ├── handlers.py         # event -> notification (idempotent)
│       │   ├── worker.py           # Kafka consumer loop
│       │   └── main.py             # read API
│       ├── migrations/
│       ├── tests/
│       └── pytest.ini
└── README.md
```

## Roadmap

- [x] Requirements and architecture
- [x] First service: models, migrations, create business/queue, join queue
- [x] Call next (row lock on the queue)
- [x] Serve, no-show, leave (state machine + atomic compare-and-set)
- [x] Dockerize the Queue Service (image, migration job, healthcheck)
- [x] Kafka: broker (KRaft), topic, producer, domain events from the Queue Service
- [x] Kafka: first consumer service (Notification), consumer groups, manual commits, idempotency, rebalancing
- [x] Dev workflow: live code reload in Docker (compose override)
- [ ] Delivery semantics, retries, dead-letter topics, idempotency
- [ ] Outbox pattern, database per service, sagas
- [ ] Event schemas and versioning
- [ ] Observability, testing, load testing, security
- [ ] Kubernetes deployment and production architecture
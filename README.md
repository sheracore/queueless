# QueueLess

Smart virtual and physical queueing for businesses such as clinics, barbers, restaurants and government offices.
Customers take a ticket (virtually, or by scanning a QR code on site), track their position and estimated wait,
and get notified when their turn approaches. Businesses create queues, call the next customer, and mark them served or skipped.

QueueLess is also a hands-on learning project: a real product used to learn **microservices, FastAPI and Kafka**
step by step, one concept at a time.

> **Status:** early development. The Queue Service runs in Docker and publishes domain events to Kafka. The consuming services come next (see Roadmap).

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

- **Producer settings:** `acks=all` and `enable.idempotence=true`; one producer per process; buffered messages are flushed on shutdown.
- **Known limitation (dual write):** the database commit and the Kafka publish are two separate writes. If Kafka is unreachable, the API still succeeds and the event is lost after `message.timeout.ms`, with only an error in the log. This will be fixed by the **outbox pattern** (see Roadmap).

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
| `kafka-ui` | Optional web UI on http://localhost:8080, only with `docker compose --profile tools up` |

Open http://localhost:8000/docs. Useful commands:

```bash
docker compose ps                       # queue-service should show "(healthy)"
docker compose logs -f queue-service
docker compose logs queue-migrate       # migration output
docker compose down                     # stop (keeps the data volume)
docker compose down -v                  # stop and wipe the database
```

Watch events live (key, partition and headers included):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic queue.events --from-beginning \
  --property print.key=true --property print.partition=true --property print.headers=true

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic queue.events
```

### Option B: run the service locally (fast reload while coding)

```bash
# 1. Start PostgreSQL, Kafka and the topic job
docker compose up -d postgres kafka kafka-init

# 2. Create a virtualenv and install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r services/queue_service/requirements-dev.txt

# 3. Configure the environment: create .env in the repo root
echo 'DATABASE_URL=postgresql+psycopg://queueless:queueless@localhost:5433/queueless' > .env

# 4. Apply migrations and run the service
cd services/queue_service
alembic upgrade head
uvicorn app.main:app --reload
```

Don't run options A and B at the same time: both use port 8000.

### Configuration

Settings are read from **environment variables**. `.env` is only a local-development convenience: `config.py` walks up from the
service directory and loads the first `.env` it finds. The Docker image never contains `.env` (see `.dockerignore`), and Compose
passes `DATABASE_URL` directly.

| Variable | Local (Option B) | Inside Docker (Option A) |
| -------- | ---------------- | ------------------------ |
| `DATABASE_URL` | `...@localhost:5433/queueless` | `...@postgres:5432/queueless` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` (default) | `kafka:9092` |
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

Tests never touch Kafka: `conftest.py` overrides the `get_publisher` dependency with a `FakePublisher` that records events in a list.

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

## Project structure

```
queueless/
├── docker-compose.yml          # postgres + queue-migrate + queue-service
├── services/
│   └── queue_service/
│       ├── Dockerfile
│       ├── .dockerignore
│       ├── requirements.txt        # runtime dependencies (go into the image)
│       ├── requirements-dev.txt    # + test tools
│       ├── alembic.ini
│       ├── app/
│       │   ├── api/routes/     # HTTP routes
│       │   ├── events/         # event envelope + Kafka publisher
│       │   ├── models/         # SQLAlchemy models
│       │   ├── schemas/        # Pydantic request/response schemas
│       │   ├── services/       # business logic
│       │   ├── config.py
│       │   ├── database.py
│       │   └── main.py
│       ├── migrations/         # Alembic
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
- [ ] Kafka: first consumer service (Notification), consumer groups, offsets
- [ ] Delivery semantics, retries, dead-letter topics, idempotency
- [ ] Outbox pattern, database per service, sagas
- [ ] Event schemas and versioning
- [ ] Observability, testing, load testing, security
- [ ] Kubernetes deployment and production architecture
# QueueLess
 
Smart virtual and physical queueing for businesses such as clinics, barbers, restaurants and government offices.
Customers take a ticket (virtually, or by scanning a QR code on site), track their position and estimated wait,
and get notified when their turn approaches. Businesses create queues, call the next customer, and mark them served or skipped.
 
QueueLess is also a hands-on learning project: a real product used to learn **microservices, FastAPI and Kafka**
step by step, one concept at a time.
 
> **Status:** early development. Only the Queue Service exists so far; Kafka and the other services come later (see Roadmap).
 
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
 
- **Stack:** FastAPI, SQLAlchemy 2.x, Alembic, Pydantic v2 / pydantic-settings, PostgreSQL 17 (Docker), pytest
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
 
Prerequisites: Python 3.12, Docker.
 
```bash
# 1. Start PostgreSQL (published on host port 5433)
docker compose up -d postgres
 
# 2. Create a virtualenv and install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
 
# 3. Configure the environment: create .env in the repo root
echo 'DATABASE_URL=postgresql+psycopg://queueless:queueless@localhost:5433/queueless' > .env
 
# 4. Apply migrations and run the service
cd services/queue_service
alembic upgrade head
uvicorn app.main:app --reload
```
 
Open http://127.0.0.1:8000/docs.
 
### Tests
 
```bash
cd services/queue_service
python -m pytest -q
```
 
Tests run against in-memory SQLite. SQLite ignores row locks, so they verify logic but not concurrency;
locking behaviour is checked manually against PostgreSQL. Partial indexes declare both `postgresql_where` and
`sqlite_where`, so the same rule holds in tests and in production.

| File | Covers |
| ---- | ------ |
| `tests/test_queue_flow.py` | join, duplicate join, 404s, call-next order, empty queue, called user can't rejoin |
| `tests/test_entry_transitions.py` | serve, no-show, leave + rejoin, final states, invalid moves, ownership, cross-queue 404 |

### Manual concurrency checks (PostgreSQL)

```bash
# Two employees press "call next" at once: expect two different tickets
curl -s -X POST localhost:8000/queues/1/call-next & curl -s -X POST localhost:8000/queues/1/call-next & wait

# Serve and no-show race on one CALLED ticket: expect one 200 and one 409
curl -s -X POST localhost:8000/queues/1/entries/1/serve & curl -s -X POST localhost:8000/queues/1/entries/1/no-show & wait
```
 
### Migrations
 
```bash
cd services/queue_service
alembic revision --autogenerate -m "describe the change"   # always review the generated file
alembic upgrade head
```
 
## Project structure
 
```
queueless/
├── docker-compose.yml
├── requirements.txt
├── services/
│   └── queue_service/
│       ├── app/
│       │   ├── api/routes/     # HTTP routes
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
- [ ] Dockerize the Queue Service
- [ ] Kafka: producers, consumers, topics, partitions, consumer groups
- [ ] Delivery semantics, retries, dead-letter topics, idempotency
- [ ] Outbox pattern, database per service, sagas
- [ ] Event schemas and versioning
- [ ] Observability, testing, load testing, security
- [ ] Kubernetes deployment and production architecture
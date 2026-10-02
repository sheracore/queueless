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
- **Rules enforced in the database:** unique ticket number per queue; one active (WAITING/CALLED) entry per user per queue
### API
 
| Method | Path | Description |
| ------ | ---- | ----------- |
| GET | `/health` | Health check |
| POST | `/businesses` | Create a business |
| POST | `/businesses/{business_id}/queues` | Create a queue (starts OPEN) |
| POST | `/queues/{queue_id}/join` | Join a queue; body `{"user_id": 1}` (temporary until auth exists) |
| POST | `/queues/{queue_id}/call-next` | Call the lowest waiting ticket |
 
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
locking behaviour is checked manually against PostgreSQL.
 
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
│       └── tests/
└── README.md
```
 
## Roadmap
 
- [x] Requirements and architecture
- [x] First service: models, migrations, create business/queue, join queue
- [ ] Call next, serve, no-show, leave (atomic state transitions)
- [ ] Dockerize the Queue Service
- [ ] Kafka: producers, consumers, topics, partitions, consumer groups
- [ ] Delivery semantics, retries, dead-letter topics, idempotency
- [ ] Outbox pattern, database per service, sagas
- [ ] Event schemas and versioning
- [ ] Observability, testing, load testing, security
- [ ] Kubernetes deployment and production architecture
 
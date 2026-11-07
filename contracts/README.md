# Event contracts

This folder is the **public contract** of every Kafka topic in QueueLess. Other services (and other teams) read it
instead of reading our code.

```
contracts/
├── queue.events/                     # produced by queue-service
│   ├── CustomerJoinedQueue.schema.json   # JSON Schema (draft 2020-12)
│   ├── CustomerJoinedQueue.example.json  # one valid message, used by consumer tests
│   └── ...
└── payment.events/                   # produced by payment-service
    └── ...
```

## Rules

1. **Never edit these files by hand.** They are generated from the producer's Pydantic models
   (`services/<service>/app/events/catalog.py`):

```bash
   python tools/schemas.py export --service services/queue_service
   python tools/schemas.py export --service services/payment_service
```

2. **Commit the generated files together with the catalog change.** The test suites run
   `python tools/schemas.py check --service services/<name>` and fail if the files are out of date (schema drift).

3. **Only BACKWARD compatible changes inside one `event_version`:**

   | Change | Allowed? |
   | ------ | -------- |
   | Add an **optional** field (with a default) | yes |
   | Add a new event type (new file) | yes |
   | Add a required field | no |
   | Remove or rename a field | no |
   | Change a field's type (`int` → `string`) | no |
   | Make an enum / `Literal` smaller | no |

   A breaking change needs a new major version: `event_version: 2`, published **next to** version 1 until every
   consumer has moved (dual publish), or a new topic.

4. **Producer schemas are closed** (`additionalProperties: false`). This is what allows "add an optional field"
   under the registry's BACKWARD rule. Consumers stay **tolerant readers**: they ignore fields they do not know.

## Schema Registry

Each file is registered as subject `<topic>-<EventType>` (TopicRecordNameStrategy), for example
`queue.events-TicketReserved`, with compatibility `BACKWARD`.

```bash
python tools/schemas.py compat   --registry http://localhost:8081   # check only, exit 1 if incompatible
python tools/schemas.py register --registry http://localhost:8081   # check, then register new versions
curl -s localhost:8081/subjects | python -m json.tool
```

In Docker the one-shot job `schema-register` runs `register` before the outbox relays start, so an incompatible
schema stops the deployment before any event is published.
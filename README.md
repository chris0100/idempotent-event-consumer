# Idempotent Event Consumer

How to process messages from SQS/SNS or Kafka **at-least-once** without duplicating side effects.
The broker is simulated in memory so the logic can be tested without AWS or Docker.

## What it shows

| Concern | Where |
|---------|-------|
| At-least-once delivery, redelivery after a failure | `InMemoryQueue` |
| Idempotency with `IN_PROGRESS` / `DONE` states | `IdempotencyStore`, `IdempotentConsumer` |
| Retry transient errors, send poison messages to a DLQ | `IdempotentConsumer` |
| Business handler kept separate from infrastructure | `MessageHandler`, `PaymentHandler` |

## Flow

```
receive -> claim(messageId) ──already DONE──> ack (skip duplicate)
                │
                └─ claimed -> handle -> markDone -> ack
                                 │
                                 └─ fails -> release claim -> nack (redeliver)
                                            after maxAttempts -> DLQ
```

## Run

```bash
mvn test
```

## Interview notes

- SQS standard queues and Kafka (by default) are **at-least-once**: duplicates will happen. The consumer must be idempotent.
- Mark `DONE` **after** the side effect succeeds. If you mark it at the start and the process dies, the event is lost.
- `IN_PROGRESS` with a lease/TTL prevents two consumers processing the same message at the same time.
- In production the store is DynamoDB (conditional put), Redis (`SET NX PX`) or a unique constraint in the database.
- The DLQ needs an alarm (e.g. CloudWatch on `ApproximateNumberOfMessagesVisible`) and a redrive process.
- SQS: visibility timeout must be longer than the processing time; `maxReceiveCount` on the redrive policy sends to the DLQ.

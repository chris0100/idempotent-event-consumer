package dev.chris0100.consumer;

import java.util.Optional;

/** Polls the queue and applies the handler exactly once per message id. */
public final class IdempotentConsumer {

    private final InMemoryQueue queue;
    private final IdempotencyStore store;
    private final MessageHandler handler;

    public IdempotentConsumer(InMemoryQueue queue, IdempotencyStore store, MessageHandler handler) {
        this.queue = queue;
        this.store = store;
        this.handler = handler;
    }

    /** Processes until the queue is empty. Returns how many messages were received. */
    public int drain() {
        int received = 0;
        Optional<Message> next;
        while ((next = queue.receive()).isPresent()) {
            received++;
            process(next.get());
        }
        return received;
    }

    void process(Message message) {
        if (!store.tryClaim(message.id())) {
            return; // duplicate: already done (or being processed by another consumer) -> ack and skip
        }
        try {
            handler.handle(message);
            store.markDone(message.id()); // only after the side effect succeeded
        } catch (Exception ex) {
            store.release(message.id());
            queue.nack(message); // redeliver; after maxReceiveCount it goes to the DLQ
        }
    }
}

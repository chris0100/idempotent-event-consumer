package dev.chris0100.consumer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class IdempotentConsumerTest {

    @Test
    void duplicateDeliveryChargesOnlyOnce() {
        InMemoryQueue queue = new InMemoryQueue(3);
        IdempotencyStore store = new IdempotencyStore();
        PaymentHandler payments = new PaymentHandler();

        queue.send("msg-1", "customer-7:5000");
        queue.sendDuplicate("msg-1", "customer-7:5000");

        new IdempotentConsumer(queue, store, payments).drain();

        assertEquals(5000L, payments.chargedTo("customer-7"));
        assertEquals(IdempotencyStore.Status.DONE, store.status("msg-1"));
    }

    @Test
    void transientFailureIsRetriedAndThenSucceeds() {
        InMemoryQueue queue = new InMemoryQueue(3);
        IdempotencyStore store = new IdempotencyStore();
        AtomicInteger attempts = new AtomicInteger();

        MessageHandler flaky = message -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("downstream timeout");
            }
        };
        queue.send("msg-2", "anything");

        new IdempotentConsumer(queue, store, flaky).drain();

        assertEquals(2, attempts.get());
        assertEquals(IdempotencyStore.Status.DONE, store.status("msg-2"));
        assertTrue(queue.deadLetters().isEmpty());
    }

    @Test
    void poisonMessageEndsInTheDlqAfterMaxAttempts() {
        InMemoryQueue queue = new InMemoryQueue(3);
        IdempotencyStore store = new IdempotencyStore();
        PaymentHandler payments = new PaymentHandler();

        queue.send("msg-3", "this-is-not-a-payment");

        new IdempotentConsumer(queue, store, payments).drain();

        assertEquals(1, queue.deadLetters().size());
        assertEquals(3, queue.deadLetters().get(0).deliveryCount());
        assertNull(store.status("msg-3"), "claim must be released so an operator can redrive it");
    }
}

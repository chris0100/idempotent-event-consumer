package dev.chris0100.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;

/** Tiny at-least-once queue: nack puts the message back; messages over maxReceiveCount go to the DLQ. */
public final class InMemoryQueue {

    private final ConcurrentLinkedDeque<Message> messages = new ConcurrentLinkedDeque<>();
    private final List<Message> deadLetters = new ArrayList<>();
    private final int maxReceiveCount;

    public InMemoryQueue(int maxReceiveCount) {
        this.maxReceiveCount = maxReceiveCount;
    }

    public void send(String id, String body) {
        messages.addLast(new Message(id, body, 1));
    }

    /** Simulates the broker delivering the same message twice (e.g. visibility timeout expired). */
    public void sendDuplicate(String id, String body) {
        send(id, body);
    }

    public Optional<Message> receive() {
        return Optional.ofNullable(messages.pollFirst());
    }

    public void nack(Message message) {
        if (message.deliveryCount() >= maxReceiveCount) {
            synchronized (deadLetters) {
                deadLetters.add(message);
            }
        } else {
            messages.addLast(message.redelivered());
        }
    }

    public List<Message> deadLetters() {
        synchronized (deadLetters) {
            return List.copyOf(deadLetters);
        }
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }
}

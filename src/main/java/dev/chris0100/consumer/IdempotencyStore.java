package dev.chris0100.consumer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Records which messages were processed. In production: DynamoDB conditional put,
 * Redis SET NX, or a unique key in the database.
 */
public final class IdempotencyStore {

    public enum Status { IN_PROGRESS, DONE }

    private final ConcurrentHashMap<String, Status> entries = new ConcurrentHashMap<>();

    /** @return true if this caller now owns the message; false if it is in progress or already done. */
    public boolean tryClaim(String messageId) {
        return entries.putIfAbsent(messageId, Status.IN_PROGRESS) == null;
    }

    public void markDone(String messageId) {
        entries.put(messageId, Status.DONE);
    }

    /** Releases a claim after a failure so a redelivery can try again. */
    public void release(String messageId) {
        entries.remove(messageId, Status.IN_PROGRESS);
    }

    public Status status(String messageId) {
        return entries.get(messageId);
    }
}

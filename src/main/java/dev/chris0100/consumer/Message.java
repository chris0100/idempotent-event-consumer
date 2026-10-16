package dev.chris0100.consumer;

/** A message as delivered by the broker. deliveryCount starts at 1. */
public record Message(String id, String body, int deliveryCount) {
    public Message redelivered() {
        return new Message(id, body, deliveryCount + 1);
    }
}

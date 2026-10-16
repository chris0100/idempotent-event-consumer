package dev.chris0100.consumer;

@FunctionalInterface
public interface MessageHandler {
    void handle(Message message) throws Exception;
}

package dev.chris0100.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Example side effect: charging a customer. Charging twice would be a real bug. */
public final class PaymentHandler implements MessageHandler {

    private final Map<String, AtomicLong> chargedCents = new ConcurrentHashMap<>();

    @Override
    public void handle(Message message) {
        // body format: customerId:amountInCents
        String[] parts = message.body().split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("malformed payment: " + message.body());
        }
        chargedCents.computeIfAbsent(parts[0], k -> new AtomicLong()).addAndGet(Long.parseLong(parts[1]));
    }

    public long chargedTo(String customerId) {
        AtomicLong value = chargedCents.get(customerId);
        return value == null ? 0 : value.get();
    }
}

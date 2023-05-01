package com.anu.messaging;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusMessageBatch;
import com.azure.messaging.servicebus.ServiceBusSenderClient;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public final class MeasureBatchSendAllocation {
    public static void main(String[] args) throws InterruptedException, IOException {

        final String conStr = System.getenv("CON_STR");
        final String qname = System.getenv("Q_NAME"); // Max-Message-Size:25MB

        ServiceBusSenderClient sender = new ServiceBusClientBuilder()
            .connectionString(conStr)
            .sender()
            .topicName(qname)
            .buildClient();

        final int concurrency = 20;
        final int warmupRuns = 10;
        final int profilingRuns = 2000;
        final List<ServiceBusMessage> testMessages = testMessages();

        System.out.println("Warmup: Start..");
        concurrentSend(sender, concurrency, warmupRuns, testMessages);
        System.out.println("Warmup: End.. Press a key when ready to Profile..");

        System.in.read();

        System.out.println("ProfileRun: Start..");
        concurrentSend(sender, concurrency, profilingRuns, testMessages);
        System.out.println("ProfileRun: End..");

        sender.close();
    }

    private static void concurrentSend(ServiceBusSenderClient sender, int concurrency, int runs, List<ServiceBusMessage> testMessages) throws InterruptedException {
        for (int run = 0; run < runs; run++) {
            final int iterations = 1000;
            final CountDownLatch latch = new CountDownLatch(iterations);
            final AtomicInteger errorCount = new AtomicInteger();

            System.out.println("Sending iterations: Start.. (" + run + ")");
            final Scheduler scheduler = Schedulers.newBoundedElastic(20, 100000, "send-worker");
            for (int i = 0; i < iterations; i++) {
                scheduler.schedule(() -> {
                    try {
                        send(sender, testMessages);
                    } catch (RuntimeException __) {
                        errorCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            System.out.println("Sending iterations: Scheduled.. Now waiting (" + run + ")");
            latch.await();
            System.out.println("Sending iterations: End...(" + run + ") Error-Count:" + errorCount.get());
        }
    }

    private static void send(ServiceBusSenderClient sender, List<ServiceBusMessage> testMessages) {
        ServiceBusMessageBatch currentBatch = sender.createMessageBatch();
        for (ServiceBusMessage message : testMessages) {
            if (currentBatch.tryAddMessage(message)) {
                continue;
            }
            sender.sendMessages(currentBatch);
            currentBatch = sender.createMessageBatch();
            if (!currentBatch.tryAddMessage(message)) {
                System.err.printf("Message is too large for an empty batch. Skipping. Max size: %s. Message: %s%n",
                    currentBatch.getMaxSizeInBytes(), message.getBody().toString());
            }
        }
        sender.sendMessages(currentBatch);
    }

    private static List<ServiceBusMessage> testMessages() {
        final String message1 = "Depend on Service Bus when you need highly reliable cloud messaging service between applications"
            + " and services even when they’re offline. Available in every Azure region, this fully managed service eliminates"
            + " the burdens of server management and licensing. Get more flexibility when brokering messaging between client and"
            + " server with asynchronous operations along with structured first-in, first-out (FIFO) messaging and publish/subscribe"
            + " capabilities. Fully enterprise messaging service jms support..";
        final String message2 = "dens of server management and licensing. Get more flexibility when brokering messaging between client and"
                + " server with asynchronous operations along with structured first-in, first-out (FIFO) messaging and publish/subscribe"
                + " capabilities. Fully enterprise messaging service jms support..";
        List<ServiceBusMessage> testMessages = Arrays.asList(
            new ServiceBusMessage(BinaryData.fromString(message1)),
            new ServiceBusMessage(BinaryData.fromString(message2)),
            new ServiceBusMessage(BinaryData.fromString("Blue")),
            new ServiceBusMessage(BinaryData.fromString("Orange")));
        testMessages.get(0).setTimeToLive(Duration.ofSeconds(1));
        return testMessages; // The batch of messages that when encoded comes around 1024 bytes (1KB).
    }
}
/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.services.pubsub.inprocess;

import com.dremio.common.util.Closeable;
import com.dremio.context.RequestContext;
import com.dremio.options.OptionManager;
import com.dremio.services.pubsub.MessageAckStatus;
import com.dremio.services.pubsub.MessageConsumer;
import com.dremio.services.pubsub.MessageContainerBase;
import com.dremio.services.pubsub.MessagePublisher;
import com.dremio.services.pubsub.MessagePublisherOptions;
import com.dremio.services.pubsub.MessageSubscriber;
import com.dremio.services.pubsub.MessageSubscriberOptions;
import com.dremio.services.pubsub.PubSubClient;
import com.dremio.services.pubsub.PubSubTracerDecorator;
import com.dremio.services.pubsub.Subscription;
import com.dremio.services.pubsub.Topic;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.Message;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.apache.commons.lang3.reflect.ConstructorUtils;

/**
 * In-process pubsub implementation intended for decoupling of notification processing from the
 * publishers of the notifications. The messages are stored in a queue during publish call and are
 * processed on an executor asynchronously. The {@link RequestContext} is passed from the publisher
 * and can be used by {@link MessageConsumer} to run processing in the same context.
 *
 * <p>There is no message redelivery unless messages are nacked. Ack() has no effect as there is no
 * storage for messages and the messages are removed from the queue before the consumer is called.
 *
 * <p>Work stealing executor is used to avoid blocking, if a thread is waiting (e.g. for an IO
 * operation), another thread may be added to maintain the level of parallelism requested. One
 * thread in the pool is reserved for polling from the queues.
 */
public class InProcessPubSubClient implements PubSubClient, Closeable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(InProcessPubSubClient.class);

  private static final String INSTRUMENTATION_SCOPE_NAME = "dremio.pubsub.inprocess";

  private static final Random random = new Random();

  private final OptionManager optionManager;
  private final Tracer tracer;
  @Nullable private final InProcessPubSubEventListener eventListener;

  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final ExecutorService executorService;
  private final Map<String, Publisher<? extends Message>> publisherByTopicName = new HashMap<>();
  private final Map<String, ArrayBlockingQueue<MessageContainer<?>>> queuesByTopicName =
      new HashMap<>();
  private final BoundedBlockingPriorityQueue<MessageContainer<?>> redeliveryQueue;
  private final Multimap<String, String> subscriptionNamesByTopicName = ArrayListMultimap.create();
  private final Map<String, Subscriber<? extends Message>> subscribersBySubscriptionName =
      new HashMap<>();
  private final AutoResetEvent publishEvent = new AutoResetEvent();
  private final Semaphore messagesInProcessingSemaphore;

  @Inject
  public InProcessPubSubClient(
      OptionManager optionManager,
      OpenTelemetry openTelemetry,
      @Nullable InProcessPubSubEventListener eventListener) {
    this.optionManager = optionManager;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE_NAME);
    this.eventListener = eventListener;

    this.executorService =
        Executors.newWorkStealingPool(
            Math.max(2, (int) optionManager.getOption(InProcessPubSubClientOptions.PARALLELISM)));
    this.redeliveryQueue =
        new BoundedBlockingPriorityQueue<>(
            (int) optionManager.getOption(InProcessPubSubClientOptions.MAX_REDELIVERY_MESSAGES),
            Comparator.comparingLong(MessageContainer::getRedeliverTimeMillis));
    this.messagesInProcessingSemaphore =
        new Semaphore(
            (int) optionManager.getOption(InProcessPubSubClientOptions.MAX_MESSAGES_IN_PROCESSING));

    // Use one thread for polling.
    executorService.submit(this::queueProcessingThread);
  }

  @Override
  public <M extends Message> MessagePublisher<M> getPublisher(
      Class<? extends Topic<M>> topicClass, MessagePublisherOptions options) {
    Topic<M> topic;
    try {
      topic = ConstructorUtils.invokeConstructor(topicClass);
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | InstantiationException e) {
      throw new RuntimeException(e);
    }

    synchronized (publisherByTopicName) {
      if (publisherByTopicName.containsKey(topic.getName())) {
        throw new RuntimeException(
            String.format("Publisher for topic %s is already registered", topic.getName()));
      }

      Publisher<M> publisher = new Publisher<M>(topic.getName());
      publisherByTopicName.put(topic.getName(), publisher);
      return publisher;
    }
  }

  @Override
  public <M extends Message> MessageSubscriber<M> getSubscriber(
      Class<? extends Subscription<M>> subscriptionClass,
      MessageConsumer<M> messageConsumer,
      MessageSubscriberOptions options) {
    Subscription<M> subscription;
    Topic<M> topic;
    try {
      subscription = ConstructorUtils.invokeConstructor(subscriptionClass);
      topic = ConstructorUtils.invokeConstructor(subscription.getTopicClass());
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | InstantiationException e) {
      throw new RuntimeException(e);
    }

    // Add subscriber. It's activated by its start() method.
    synchronized (subscribersBySubscriptionName) {
      if (subscribersBySubscriptionName.containsKey(subscription.getName())) {
        throw new RuntimeException(
            String.format(
                "Subscriber for subscription %s is already registered", subscription.getName()));
      }
      Subscriber<M> subscriber =
          new Subscriber<>(topic.getName(), subscription.getName(), messageConsumer);
      subscribersBySubscriptionName.put(subscription.getName(), subscriber);
      return subscriber;
    }
  }

  @Override
  public void close() {
    shutdownLatch.countDown();
    try {
      executorService.awaitTermination(
          optionManager.getOption(InProcessPubSubClientOptions.TERMINATION_TIMEOUT_MILLIS),
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while shutting down", e);
    }
  }

  private void onPublish(
      String topicName, int queueLength, boolean success, @Nullable String exceptionName) {
    if (eventListener != null) {
      eventListener.onPublish(topicName, queueLength, success, exceptionName);
    }
  }

  private void onMessageReceived(
      String topicName, String subscriptionName, boolean success, @Nullable String exceptionName) {
    if (eventListener != null) {
      eventListener.onMessageReceived(topicName, subscriptionName, success, exceptionName);
    }
  }

  /** Iterates over messages in the message queues and processes them on the executor. */
  private void queueProcessingThread() {
    try {
      while (!shutdownLatch.await(0, TimeUnit.MILLISECONDS)) {
        // Continue waiting if no messages to process.
        publishEvent.waitEvent(
            optionManager.getOption(InProcessPubSubClientOptions.QUEUE_POLL_MILLIS));

        // Iterate over all topics, poll messages up to a limit and submit for execution.
        // Ok to do it under a lock as it only queues the tasks w/o running them.
        boolean setPublishEvent = false;
        synchronized (queuesByTopicName) {
          for (Map.Entry<String, ArrayBlockingQueue<MessageContainer<?>>> entry :
              queuesByTopicName.entrySet()) {
            long maxMessagesToPoll =
                optionManager.getOption(InProcessPubSubClientOptions.MAX_MESSAGES_TO_POLL);
            while (!entry.getValue().isEmpty() && maxMessagesToPoll-- > 0) {
              // The peek/poll logic here won't let parallelize this method as poll may return a
              // different result if multiple threads poll from the queues.
              ArrayBlockingQueue<MessageContainer<?>> queue = entry.getValue();
              MessageContainer<?> messageContainer = queue.peek();
              Subscriber<?> subscriber;
              synchronized (subscribersBySubscriptionName) {
                subscriber =
                    subscribersBySubscriptionName.get(messageContainer.getSubscriptionName());
              }
              if (subscriber != null) {
                // Try to acquire permit to process the message.
                if (messagesInProcessingSemaphore.tryAcquire(
                    optionManager.getOption(InProcessPubSubClientOptions.QUEUE_POLL_MILLIS),
                    TimeUnit.MILLISECONDS)) {
                  final MessageContainer<?> messageContainer1 = queue.poll();
                  executorService.submit(() -> subscriber.processMessage(messageContainer1));
                }
              }
            }

            if (!entry.getValue().isEmpty()) {
              setPublishEvent = true;
            }
          }
        }

        // Re-add messages from the redelivery queue.
        long currentTimeMillis = System.currentTimeMillis();
        while (!redeliveryQueue.isEmpty()
            && redeliveryQueue.peek().getRedeliverTimeMillis() <= currentTimeMillis) {
          MessageContainer<?> messageContainer = redeliveryQueue.take();
          synchronized (queuesByTopicName) {
            queuesByTopicName.get(messageContainer.getTopicName()).put(messageContainer);
          }
        }

        // If any of the queues is not empty, immediately start next processing iteration.
        if (setPublishEvent || !redeliveryQueue.isEmpty()) {
          publishEvent.set();
        }
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted queue processing", e);
    }
  }

  /**
   * The publisher adds {@link MessageContainer}s, one for every subscriber registered at the time
   * of message publish.
   */
  private final class Publisher<M extends Message> implements MessagePublisher<M> {
    private final String topicName;

    private Publisher(String topicName) {
      this.topicName = topicName;
    }

    @Override
    public CompletableFuture<String> publish(M message) {
      return PubSubTracerDecorator.addPublishTracing(
          tracer,
          topicName,
          (span) -> {
            try {
              if (shutdownLatch.await(0, TimeUnit.MILLISECONDS)) {
                throw new RuntimeException("The pubsub client was stopped");
              }
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }

            try {
              // Build messages per subscription.
              String messageId = UUID.randomUUID().toString();
              RequestContext requestContext = RequestContext.current();
              List<MessageContainer<M>> messageContainersToAdd = new ArrayList<>();
              synchronized (subscriptionNamesByTopicName) {
                for (String subscriptionName : subscriptionNamesByTopicName.get(topicName)) {
                  messageContainersToAdd.add(
                      new MessageContainer<>(
                          topicName, subscriptionName, messageId, message, requestContext));
                }
              }

              // Add messages to the processing queue.
              if (!messageContainersToAdd.isEmpty()) {
                ArrayBlockingQueue<MessageContainer<?>> queue;
                synchronized (queuesByTopicName) {
                  // Use a blocking queue to guard against OOM.
                  queue =
                      queuesByTopicName.computeIfAbsent(
                          topicName,
                          (key) ->
                              new ArrayBlockingQueue<>(
                                  (int)
                                      optionManager.getOption(
                                          InProcessPubSubClientOptions.MAX_MESSAGES_IN_QUEUE)));
                }
                for (MessageContainer<M> container : messageContainersToAdd) {
                  try {
                    queue.put(container);
                  } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting to put message in queue", e);
                    throw new RuntimeException(e);
                  }
                }

                // Notify queue processing thread that new messages are available.
                publishEvent.set();

                // Notify listener.
                onPublish(topicName, queue.size(), true, null);
              }

              return CompletableFuture.completedFuture(messageId);
            } catch (Exception e) {
              // Notify listener.
              Queue<MessageContainer<?>> queue = queuesByTopicName.get(topicName);
              onPublish(
                  topicName, queue != null ? queue.size() : 0, false, e.getClass().getSimpleName());
              throw e;
            }
          });
    }

    @Override
    public void close() {
      synchronized (publisherByTopicName) {
        publisherByTopicName.remove(topicName);
      }
    }
  }

  /** Subscriber adds/removes itself in start/close and processes incoming messages. */
  private final class Subscriber<M extends Message> implements MessageSubscriber<M> {

    private final String topicName;
    private final String subscriptionName;
    private final MessageConsumer<M> messageConsumer;

    private Subscriber(
        String topicName, String subscriptionName, MessageConsumer<M> messageConsumer) {
      this.topicName = topicName;
      this.subscriptionName = subscriptionName;
      this.messageConsumer = messageConsumer;
    }

    private String getSubscriptionName() {
      return subscriptionName;
    }

    private void processMessage(MessageContainer<?> messageContainer) {
      PubSubTracerDecorator.addSubscribeTracing(
          tracer,
          subscriptionName,
          Context.current(),
          (span) -> {
            try {
              messageConsumer.process((MessageContainer<M>) messageContainer);

              // Notify listener.
              onMessageReceived(topicName, subscriptionName, true, null);
            } catch (Exception e) {
              span.recordException(e);

              // Notify listener.
              onMessageReceived(topicName, subscriptionName, false, e.getClass().getSimpleName());

              // All uncaught exceptions result in an ack, the message consumer must decide how to
              // handle
              // exceptions otherwise.
              messageContainer.ack();
            } finally {
              // Release the permit.
              messagesInProcessingSemaphore.release();
            }
            return null;
          });
    }

    @Override
    public void start() {
      // Add subscription name to the map for listening.
      synchronized (subscriptionNamesByTopicName) {
        subscriptionNamesByTopicName.put(topicName, subscriptionName);
      }
    }

    @Override
    public void close() {
      // Remove this subscriber from listening.
      synchronized (subscriptionNamesByTopicName) {
        subscriptionNamesByTopicName.remove(topicName, subscriptionName);
      }

      // Remove subscriber.
      synchronized (subscribersBySubscriptionName) {
        subscribersBySubscriptionName.remove(subscriptionName);
      }
    }
  }

  /** Message container with ack/nack methods. */
  private final class MessageContainer<M extends Message> extends MessageContainerBase<M> {
    private final String topicName;
    private final String subscriptionName;
    private long remainingRedeliveryAttempts =
        optionManager.getOption(InProcessPubSubClientOptions.MAX_REDELIVERY_ATTEMPTS);
    private long redeliverTimeMillis;

    private MessageContainer(
        String topicName,
        String subscriptionName,
        String id,
        M message,
        RequestContext requestContext) {
      super(id, message, requestContext);
      this.topicName = topicName;
      this.subscriptionName = subscriptionName;
    }

    private String getTopicName() {
      return topicName;
    }

    private String getSubscriptionName() {
      return subscriptionName;
    }

    private void setRedeliverTimeMillis() {
      long millis = System.currentTimeMillis();
      long minMillis =
          millis
              + 1000
                  * optionManager.getOption(
                      InProcessPubSubClientOptions.MIN_DELAY_FOR_REDELIVERY_SECONDS);
      long maxMillis =
          millis
              + 1000
                  * optionManager.getOption(
                      InProcessPubSubClientOptions.MAX_DELAY_FOR_REDELIVERY_SECONDS);
      this.redeliverTimeMillis = (long) (minMillis + (maxMillis - minMillis) * random.nextDouble());
    }

    private long getRedeliverTimeMillis() {
      return redeliverTimeMillis;
    }

    @Override
    public CompletableFuture<MessageAckStatus> ack() {
      // Nothing to do, just return success.
      return CompletableFuture.completedFuture(MessageAckStatus.SUCCESSFUL);
    }

    @Override
    public CompletableFuture<MessageAckStatus> nack() {
      if (remainingRedeliveryAttempts-- > 0) {
        // Enqueue self for re-delivery.
        setRedeliverTimeMillis();
        try {
          redeliveryQueue.offer(this);
        } catch (InterruptedException e) {
          logger.error("Interrupted while adding to redelivery queue", e);
          return CompletableFuture.completedFuture(MessageAckStatus.OTHER);
        }
        return CompletableFuture.completedFuture(MessageAckStatus.SUCCESSFUL);
      } else {
        // This message will not be re-attempted.
        return CompletableFuture.completedFuture(MessageAckStatus.FAILED_PRECONDITION);
      }
    }
  }

  /** Bounded blocking priority queue. */
  private static final class BoundedBlockingPriorityQueue<E> {
    private final PriorityBlockingQueue<E> queue;
    private final Semaphore semaphore;

    public BoundedBlockingPriorityQueue(int maxSize, Comparator<E> comparator) {
      if (maxSize <= 0) {
        throw new IllegalArgumentException("maxSize should be greater than 0");
      }
      this.queue = new PriorityBlockingQueue<>(maxSize, comparator);
      this.semaphore = new Semaphore(maxSize);
    }

    public boolean offer(E e) throws InterruptedException {
      semaphore.acquire();
      boolean wasAdded = false;
      try {
        synchronized (queue) {
          wasAdded = queue.offer(e);
        }
        return wasAdded;
      } finally {
        if (!wasAdded) {
          semaphore.release();
        }
      }
    }

    public E take() throws InterruptedException {
      E item = queue.take();
      semaphore.release();
      return item;
    }

    public E peek() {
      return queue.peek();
    }

    public boolean isEmpty() {
      return queue.isEmpty();
    }
  }

  /**
   * Similar to C# AutoReset event, this class is a simple boolean event that resets after notifying
   * subscribers.
   */
  private static final class AutoResetEvent {
    private final Object monitor = new Object();
    private volatile boolean eventSet;

    public boolean waitEvent(long millis) throws InterruptedException {
      synchronized (monitor) {
        monitor.wait(millis);
        boolean wasSet = eventSet;
        eventSet = false;
        return wasSet;
      }
    }

    public void set() {
      synchronized (monitor) {
        eventSet = true;
        monitor.notify();
      }
    }
  }
}

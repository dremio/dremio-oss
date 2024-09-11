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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators;
import com.dremio.services.pubsub.ImmutableMessagePublisherOptions;
import com.dremio.services.pubsub.ImmutableMessageSubscriberOptions;
import com.dremio.services.pubsub.MessageConsumer;
import com.dremio.services.pubsub.MessageContainerBase;
import com.dremio.services.pubsub.MessagePublisher;
import com.dremio.services.pubsub.MessageSubscriber;
import com.dremio.services.pubsub.Subscription;
import com.dremio.services.pubsub.Topic;
import com.google.protobuf.Parser;
import com.google.protobuf.Timestamp;
import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestInProcessPubSubClient {
  @Mock private OptionManager optionManager;
  @Mock private InProcessPubSubEventListener eventListener;

  private InProcessPubSubClient client;
  private MessagePublisher<Timestamp> publisher;
  private MessageSubscriber<Timestamp> subscriber;
  private final TestMessageConsumer messageConsumer = new TestMessageConsumer();

  @AfterEach
  public void tearDown() {
    publisher.close();
    subscriber.close();
  }

  /** Default mocking of the {@link OptionManager}. */
  private void mockOptionManager() {
    doAnswer(
            (args) -> {
              TypeValidators.LongValidator validator = args.getArgument(0);
              return validator.getDefault().getNumVal();
            })
        .when(optionManager)
        .getOption(any(TypeValidators.LongValidator.class));
  }

  private void startClient() {
    client = new InProcessPubSubClient(optionManager, OpenTelemetry.noop(), eventListener);
    publisher =
        client.getPublisher(
            TestTopic.class, new ImmutableMessagePublisherOptions.Builder().build());
    subscriber =
        client.getSubscriber(
            TestSubscription.class,
            messageConsumer,
            new ImmutableMessageSubscriberOptions.Builder().build());
    subscriber.start();
  }

  @Test
  public void test_shutdown() {
    mockOptionManager();
    startClient();

    // expect no exceptions
    client.close();
  }

  @Test
  public void test_cannotAddSameTopic() {
    mockOptionManager();
    startClient();

    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () ->
                client.getPublisher(
                    TestTopic.class, new ImmutableMessagePublisherOptions.Builder().build()));
    assertThat(e).hasMessage("Publisher for topic test is already registered");
  }

  @Test
  public void test_cannotAddSameSubscription() {
    mockOptionManager();
    startClient();

    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () ->
                client.getSubscriber(
                    TestSubscription.class,
                    messageConsumer,
                    new ImmutableMessageSubscriberOptions.Builder().build()));
    assertThat(e).hasMessage("Subscriber for subscription test is already registered");
  }

  @Test
  public void test_publishAndSubscribe() throws Exception {
    mockOptionManager();
    startClient();

    CountDownLatch consumerLatch = messageConsumer.initLatch(1);

    Timestamp timestamp = Timestamp.newBuilder().setSeconds(1000L).build();
    publisher.publish(timestamp);

    assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

    // Verify.
    assertThat(messageConsumer.getMessages()).hasSize(1);
    MessageContainerBase<Timestamp> messageContainer = messageConsumer.getMessages().get(0);
    assertThat(messageContainer.getMessage()).isEqualTo(timestamp);
    messageContainer.ack();

    verify(eventListener, times(1))
        .onPublish(eq(new TestTopic().getName()), anyInt(), eq(true), eq(null));
    verify(eventListener, times(1))
        .onMessageReceived(
            eq(new TestTopic().getName()),
            eq(new TestSubscription().getName()),
            eq(true),
            eq(null));
  }

  /** Test that nack results in delayed re-delivery. */
  @Test
  public void test_nack() throws Exception {
    final long minDelaySeconds = 1;
    final long maxDelaySeconds = 2;
    doAnswer(
            (args) -> {
              TypeValidators.LongValidator validator = args.getArgument(0);
              switch (validator.getOptionName()) {
                case "dremio.pubsub.inprocess.min_delay_for_redelivery_seconds":
                  return minDelaySeconds;
                case "dremio.pubsub.inprocess.max_delay_for_redelivery_seconds":
                  return maxDelaySeconds;
                default:
                  return validator.getDefault().getNumVal();
              }
            })
        .when(optionManager)
        .getOption(any(TypeValidators.LongValidator.class));
    startClient();

    CountDownLatch consumerLatch = messageConsumer.initLatch(1);

    Timestamp timestamp = Timestamp.newBuilder().setSeconds(1000L).build();
    publisher.publish(timestamp);

    assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

    assertThat(messageConsumer.getMessages()).hasSize(1);
    MessageContainerBase<Timestamp> messageContainer = messageConsumer.getMessages().get(0);

    long timeBeforeNack = System.currentTimeMillis();
    consumerLatch = messageConsumer.initLatch(1);
    messageContainer.nack();

    // Wait for redelivery.
    assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

    assertThat(messageConsumer.getMessages()).hasSize(2);
    messageContainer = messageConsumer.getMessages().get(1);
    assertThat(messageContainer.getMessage()).isEqualTo(timestamp);
    messageContainer.ack();

    // Check that redelivery was delayed by a second.
    assertThat((System.currentTimeMillis() - timeBeforeNack) / 1000)
        .isBetween(minDelaySeconds, maxDelaySeconds);
  }

  /**
   * This tests that too many publishing requests with slow processing of messages results in
   * publisher being blocked until executor service frees up.
   */
  @Test
  public void test_blockIfTooManyInProcessing() {
    final long minDelaySeconds = 1;
    final long maxDelaySeconds = 2;
    final long maxMessagesInProcessing = 1;
    doAnswer(
            (args) -> {
              TypeValidators.LongValidator validator = args.getArgument(0);
              switch (validator.getOptionName()) {
                case "dremio.pubsub.inprocess.min_delay_for_redelivery_seconds":
                  return minDelaySeconds;
                case "dremio.pubsub.inprocess.max_delay_for_redelivery_seconds":
                  return maxDelaySeconds;
                  // Set queue size and max processing count to the same value.
                case "dremio.pubsub.inprocess.max_messages_in_queue":
                case "dremio.pubsub.inprocess.max_messages_in_processing":
                  return maxMessagesInProcessing;
                default:
                  return validator.getDefault().getNumVal();
              }
            })
        .when(optionManager)
        .getOption(any(TypeValidators.LongValidator.class));
    startClient();

    // Set processing delay.
    long delayMillis = 1000;
    messageConsumer.setProcessingDelayMillis(delayMillis);

    // Publish messages:
    //  - First message blocks for a second.
    //  - Second message is put into the queue immediately but cannot get out of the queue because
    //    too many messages are being processed.
    //  - Third message cannot be put into the blocking queue as it's full, so the call to publish
    //    blocks for delayMillis at least.
    long beforePublishMillis = System.currentTimeMillis();
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(1000L).build();
    publisher.publish(timestamp);
    publisher.publish(timestamp);
    publisher.publish(timestamp);
    long afterPublishMillis = System.currentTimeMillis();

    // Verify delay.
    assertThat(afterPublishMillis - beforePublishMillis).isBetween(delayMillis, 5 * delayMillis);
  }

  /**
   * This tests that delays in synchronization primitives waits don't add up to a large delay in
   * processing.
   */
  @Test
  public void test_throughput() throws Exception {
    mockOptionManager();
    startClient();

    // Publish and wait for all to arrive.
    int messagesToPublish = 10000;
    CountDownLatch latch = messageConsumer.initLatch(messagesToPublish);
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(1000L).build();
    for (int i = 0; i < messagesToPublish; i++) {
      publisher.publish(timestamp);
    }

    // It takes ~200ms to run it, set 10x that timeout to avoid flakiness.
    // The default delay in queue processing is 10ms, which for 10K items
    // would by far exceed 2s if excessive wait existed.
    assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
  }

  public static final class TestTopic implements Topic<Timestamp> {
    @Override
    public String getName() {
      return "test";
    }

    @Override
    public Class<Timestamp> getMessageClass() {
      return Timestamp.class;
    }
  }

  public static final class TestSubscription implements Subscription<Timestamp> {
    @Override
    public String getName() {
      return "test";
    }

    @Override
    public Parser<Timestamp> getMessageParser() {
      return Timestamp.parser();
    }

    @Override
    public Class<? extends Topic<Timestamp>> getTopicClass() {
      return TestTopic.class;
    }
  }

  private static final class TestMessageConsumer implements MessageConsumer<Timestamp> {
    private long messageProcessingDelayMillis;
    private volatile CountDownLatch latch;
    private final List<MessageContainerBase<Timestamp>> messages = new ArrayList<>();

    @Override
    public void process(MessageContainerBase<Timestamp> message) {
      if (messageProcessingDelayMillis > 0) {
        try {
          Thread.sleep(messageProcessingDelayMillis);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      messages.add(message);
      latch.countDown();
    }

    private void setProcessingDelayMillis(long millis) {
      this.messageProcessingDelayMillis = millis;
    }

    private CountDownLatch initLatch(int count) {
      latch = new CountDownLatch(count);
      return latch;
    }

    private List<MessageContainerBase<Timestamp>> getMessages() {
      return messages;
    }
  }
}

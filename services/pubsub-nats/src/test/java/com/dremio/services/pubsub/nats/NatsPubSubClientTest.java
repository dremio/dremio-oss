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
package com.dremio.services.pubsub.nats;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.services.pubsub.ImmutableMessageSubscriberOptions;
import com.dremio.services.pubsub.MessageContainerBase;
import com.dremio.services.pubsub.MessagePublisher;
import com.dremio.services.pubsub.MessageSubscriber;
import com.dremio.services.pubsub.Subscription;
import com.dremio.services.pubsub.TestMessageConsumer;
import com.dremio.services.pubsub.Topic;
import com.dremio.services.pubsub.nats.integration.NatsTestStarter;
import com.dremio.services.pubsub.nats.management.ImmutableStreamOptions;
import com.google.protobuf.Parser;
import com.google.protobuf.Timestamp;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

// TODO(DX-94593): Disabled until the test-containers version will be replaced with embedded NATS
@Disabled("Disabled until the test-containers version will be replaced with embedded NATS DX-94593")
public class NatsPubSubClientTest {

  private static final NatsTestStarter natsTestStarter = new NatsTestStarter();

  @BeforeAll
  public static void setUp() throws IOException, InterruptedException, JetStreamApiException {
    natsTestStarter.setUp();
  }

  @AfterAll
  public static void tearDown() {
    natsTestStarter.tearDown();
  }

  public static final String STREAM_NAME = "TEST_STREAM";

  private MessagePublisher<Timestamp> publisher;
  private MessageSubscriber<Timestamp> subscriber;
  private TestMessageConsumer<Timestamp> messageConsumer;

  @BeforeEach
  public void createStream() throws IOException, JetStreamApiException {
    NatsPubSubClient client = new NatsPubSubClient(natsTestStarter.getNatsUrl());
    client
        .getStreamManager()
        .upsertStream(
            new ImmutableStreamOptions.Builder()
                .setStreamName(STREAM_NAME)
                .setNumberOfReplicas(1)
                .build(),
            List.of(TestTopic.class));
  }

  @AfterEach
  public void deleteStream() throws IOException, JetStreamApiException {
    // Create JetStream Management context
    JetStreamManagement jsm = natsTestStarter.getNatsConnection().jetStreamManagement();

    jsm.deleteStream(STREAM_NAME);

    subscriber.close();
    publisher.close();
  }

  @Test
  public void test_publishAndSubscribe() throws InterruptedException, ExecutionException {
    startClient(100, 1000);

    CountDownLatch consumerLatch = messageConsumer.initLatch(1);

    Timestamp timestamp =
        Timestamp.newBuilder().setSeconds(new Random().nextInt(Integer.MAX_VALUE)).build();
    CompletableFuture<String> result = publisher.publish(timestamp);
    assertThat(result.get()).isNotEmpty();

    Assertions.assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

    assertThat(messageConsumer.getMessages()).hasSize(1);
    MessageContainerBase<Timestamp> messageContainer = messageConsumer.getMessages().get(0);
    assertThat(messageContainer.getMessage()).isEqualTo(timestamp);
    messageContainer.ack();
  }

  /** Test that nack results in delayed re-delivery. */
  @Test
  public void test_nack() throws Exception {
    startClient(100, 1000);

    CountDownLatch consumerLatch = messageConsumer.initLatch(1);

    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Integer.MAX_VALUE).build();
    publisher.publish(timestamp);

    Assertions.assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));
    assertThat(messageConsumer.getMessages()).hasSize(1);
    MessageContainerBase<Timestamp> messageContainer = messageConsumer.getMessages().get(0);

    consumerLatch = messageConsumer.initLatch(1);
    System.out.println("calling nack");
    messageContainer.nack();

    // Wait for redelivery. (in NASK we cannot control how fast the NACK message is re-delivered)
    Assertions.assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

    assertThat(messageConsumer.getMessages()).hasSize(2);
    messageContainer = messageConsumer.getMessages().get(1);
    assertThat(messageContainer.getMessage()).isEqualTo(timestamp);
    messageContainer.ack();
  }

  /**
   * This tests that when the ack was not called for a message, the new message is not send when the
   * maxMessagesInProcessing is set to 1.
   */
  @Test
  public void test_blockIfTooManyInProcessing() throws InterruptedException {
    final long maxMessagesInProcessing = 1;
    startClient(maxMessagesInProcessing, 1000);
    CountDownLatch consumerLatch = messageConsumer.initLatch(2);

    // Publish two messages without ack. Customer should get only the first one
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(1000L).build();
    publisher.publish(timestamp);
    publisher.publish(timestamp);

    // it won't get two messages (only the first one)
    Assertions.assertFalse(consumerLatch.await(10, TimeUnit.SECONDS));

    // ack the first message, should get both two messages
    messageConsumer.getMessages().get(0).ack();
    Assertions.assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));
  }

  /**
   * This tests verifies that message gets re-delivered when there was no nack/ack and the
   * maxWaitTime elapsed.
   */
  @Test
  public void test_redeliversWhenNoAckNackAndWaitTimeElapsed() throws InterruptedException {
    final long maxMessagesInProcessing = 10;
    int ackWaitSeconds = 10;
    startClient(maxMessagesInProcessing, ackWaitSeconds);
    CountDownLatch consumerLatch = messageConsumer.initLatch(2);

    // Publish one message without ack/nack. Customer should get only the first one
    // and second one should be retried
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(1000L).build();
    publisher.publish(timestamp);

    // it won't get two messages (only the first one)
    Assertions.assertFalse(consumerLatch.await(10, TimeUnit.SECONDS));

    // the ackWaitSeconds elapsed, should get both two messages
    Assertions.assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));
  }

  private void startClient(long maxMessagesInProcessing, int ackWaitSeconds) {
    messageConsumer = new TestMessageConsumer<>();
    NatsPubSubClient client = new NatsPubSubClient(natsTestStarter.getNatsUrl());
    publisher = client.getPublisher(TestTopic.class, null);
    subscriber =
        client.getSubscriber(
            TestSubscription.class,
            messageConsumer,
            new ImmutableMessageSubscriberOptions.Builder()
                .setMaxAckPending(maxMessagesInProcessing)
                .setAckWait(Duration.ofSeconds(ackWaitSeconds))
                .build());

    subscriber.start();
  }

  public static final class TestTopic implements Topic<Timestamp> {
    @Override
    public String getName() {
      return "test-subject";
    }

    @Override
    public Class<Timestamp> getMessageClass() {
      return Timestamp.class;
    }
  }

  public static final class TestSubscription implements Subscription<Timestamp> {
    @Override
    public String getName() {
      return "test-subject";
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
}

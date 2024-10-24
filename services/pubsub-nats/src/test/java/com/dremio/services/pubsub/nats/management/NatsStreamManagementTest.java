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
package com.dremio.services.pubsub.nats.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.services.pubsub.ImmutableMessageSubscriberOptions;
import com.dremio.services.pubsub.MessageContainerBase;
import com.dremio.services.pubsub.MessagePublisher;
import com.dremio.services.pubsub.MessageSubscriber;
import com.dremio.services.pubsub.Subscription;
import com.dremio.services.pubsub.TestMessageConsumer;
import com.dremio.services.pubsub.Topic;
import com.dremio.services.pubsub.nats.NatsPubSubClient;
import com.dremio.services.pubsub.nats.integration.NatsTestStarter;
import com.google.protobuf.Parser;
import com.google.protobuf.StringValue;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

// TODO(DX-94593): Disabled until the test-containers version will be replaced with embedded NATS
@Disabled("Disabled until the test-containers version will be replaced with embedded NATS DX-94593")
public class NatsStreamManagementTest {

  private static final NatsTestStarter natsTestStarter = new NatsTestStarter();

  @BeforeAll
  public static void setUp() throws IOException, InterruptedException {
    natsTestStarter.setUp();
  }

  @AfterAll
  public static void tearDown() {
    natsTestStarter.tearDown();
  }

  @Test
  public void test_createStream() {
    // given
    NatsPubSubClient client = new NatsPubSubClient(natsTestStarter.getNatsUrl());

    // when
    client
        .getStreamManager()
        .upsertStream(
            new ImmutableStreamOptions.Builder()
                .setStreamName("stream_name")
                .setNumberOfReplicas(1)
                .build(),
            List.of(TestTopic.class));

    // then able to publish to a stream
    MessagePublisher<StringValue> publisher = client.getPublisher(TestTopic.class, null);
    publisher.publish(StringValue.of("Value 1"));
  }

  @Test
  public void test_updateStreamDoesNotRemoveEvents() throws InterruptedException {
    // given
    NatsPubSubClient client = new NatsPubSubClient(natsTestStarter.getNatsUrl());

    // when
    client
        .getStreamManager()
        .upsertStream(
            new ImmutableStreamOptions.Builder()
                .setStreamName("stream_name")
                .setNumberOfReplicas(1)
                .build(),
            List.of(TestTopic.class));

    // then able to publish to a stream
    MessagePublisher<StringValue> publisher = client.getPublisher(TestTopic.class, null);
    publisher.publish(StringValue.of("Value 1"));

    // when update
    client
        .getStreamManager()
        .upsertStream(
            new ImmutableStreamOptions.Builder()
                .setStreamName("stream_name")
                .setNumberOfReplicas(1)
                .build(),
            List.of(TestTopic.class, TestTopic2.class));

    // then subscriber should receive one message
    TestMessageConsumer<StringValue> messageConsumer = new TestMessageConsumer<>();
    CountDownLatch consumerLatch = messageConsumer.initLatch(1);
    MessageSubscriber<StringValue> subscriber =
        client.getSubscriber(
            TestSubscription.class,
            messageConsumer,
            new ImmutableMessageSubscriberOptions.Builder().build());

    subscriber.start();

    Assertions.assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

    assertThat(messageConsumer.getMessages()).hasSize(1);
    MessageContainerBase<StringValue> messageContainer = messageConsumer.getMessages().get(0);
    assertThat(messageContainer.getMessage()).isEqualTo(StringValue.of("Value 1"));
  }

  public static final class TestTopic implements Topic<StringValue> {
    @Override
    public String getName() {
      return "test-subject";
    }

    @Override
    public Class<StringValue> getMessageClass() {
      return StringValue.class;
    }
  }

  public static final class TestTopic2 implements Topic<StringValue> {
    @Override
    public String getName() {
      return "test-subject2";
    }

    @Override
    public Class<StringValue> getMessageClass() {
      return StringValue.class;
    }
  }

  public static final class TestSubscription implements Subscription<StringValue> {
    @Override
    public String getName() {
      return "test-subject";
    }

    @Override
    public Parser<StringValue> getMessageParser() {
      return StringValue.parser();
    }

    @Override
    public Class<? extends Topic<StringValue>> getTopicClass() {
      return TestTopic.class;
    }
  }
}

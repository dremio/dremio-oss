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

import com.dremio.context.RequestContext;
import com.dremio.services.pubsub.MessageAckStatus;
import com.dremio.services.pubsub.MessageConsumer;
import com.dremio.services.pubsub.MessageContainerBase;
import com.dremio.services.pubsub.MessageSubscriber;
import com.dremio.services.pubsub.MessageSubscriberOptions;
import com.dremio.services.pubsub.Subscription;
import com.dremio.services.pubsub.Topic;
import com.dremio.services.pubsub.nats.exceptions.NatsSubscriberException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NatsSubscriber<M extends Message> implements MessageSubscriber<M> {
  public static final Duration DEFAULT_ACK_WAIT_SECONDS = Duration.ofSeconds(10);
  public static final long DEFAULT_MAX_PENDING = 1000L;
  private final Topic<M> subject;
  private final Subscription<M> subscription;
  private final String natsServerUrl;
  private final MessageConsumer<M> messageConsumer;
  private final MessageSubscriberOptions options;
  private final ScheduledExecutorService pullExecutorService;
  private Connection natsConnection;
  private JetStream jetStream;

  public NatsSubscriber(
      Topic<M> subject,
      Subscription<M> subscription,
      String natsServerUrl,
      MessageConsumer<M> messageConsumer,
      MessageSubscriberOptions options) {
    this.subject = subject;
    this.subscription = subscription;
    this.natsServerUrl = natsServerUrl;
    this.messageConsumer = messageConsumer;
    this.options = options;
    // TODO(DX-92825): may require more threads, pick the value based on performance tests
    this.pullExecutorService = new ScheduledThreadPoolExecutor(1);
  }

  public void connect() {
    try {
      this.natsConnection = Nats.connect(natsServerUrl);
      JetStreamManagement jsm = natsConnection.jetStreamManagement();
      this.jetStream = jsm.jetStream();
    } catch (Exception e) {
      throw new NatsSubscriberException("Problem when connecting to NATS", e);
    }
  }

  /**
   * According to NATS docs: We recommend pull consumers for new projects. In particular when
   * scalability, detailed flow control or error handling are a concern.
   */
  @Override
  public void start() {
    Duration ackWait = options.ackWait().orElse(DEFAULT_ACK_WAIT_SECONDS);
    long maxAckPending = options.maxAckPending().orElse(DEFAULT_MAX_PENDING);

    // Build our subscription options. Durable is REQUIRED for pull based subscriptions
    PullSubscribeOptions pullOptions =
        PullSubscribeOptions.builder()
            .durable("my-durable-name")
            // this allows consumer to re-start and resume
            // from the last known state.
            .configuration(
                ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.Explicit)
                    .ackWait(ackWait.toMillis())
                    .maxAckPending(maxAckPending)
                    .build())
            .build();
    try {
      // TODO(DX-95075): Consider changing to the StreamContext
      JetStreamSubscription sub = jetStream.subscribe(subject.getName(), pullOptions);
      pullExecutorService.scheduleAtFixedRate(
          () -> pullMessagesFromNats(sub), 0, 5, TimeUnit.SECONDS);

    } catch (IOException | JetStreamApiException e) {
      throw new NatsSubscriberException("Problem when subscribe", e);
    }
  }

  private void pullMessagesFromNats(JetStreamSubscription sub) {
    // TODO(DX-94553): parameterize the batch size and maxWait or pick it base on perf tests
    // ways of pulling: https://nats.io/blog/jetstream-java-client-05-pull-subscribe/
    List<io.nats.client.Message> messages = sub.fetch(1000, Duration.ofSeconds(1)); //

    for (io.nats.client.Message message : messages) {
      RequestContext requestContext = RequestContext.current();
      // TODO(DX-94554): next.getSID() is a id of a subscription, not a message
      // if we want to have unique id for each message, it needs to be added explicitly

      // create a protobuf instance from bytes received from NATS
      M protoMessage = null;
      try {
        protoMessage = subscription.getMessageParser().parseFrom(message.getData());
      } catch (InvalidProtocolBufferException e) {
        throw new NatsSubscriberException("Problem when parsing message", e);
      }

      MessageContainerBase<M> natsMessageContainer =
          new NatsMessageContainer<>(message.getSID(), protoMessage, message, requestContext);
      messageConsumer.process(natsMessageContainer);
    }
  }

  private static final class NatsMessageContainer<M extends Message>
      extends MessageContainerBase<M> {

    private final io.nats.client.Message natsMessage;

    private NatsMessageContainer(
        String id,
        M protoMessage,
        io.nats.client.Message natsMessage,
        RequestContext requestContext) {
      super(id, protoMessage, requestContext);
      this.natsMessage = natsMessage;
    }

    @Override
    public CompletableFuture<MessageAckStatus> ack() {
      natsMessage.ack();
      return CompletableFuture.completedFuture(MessageAckStatus.SUCCESSFUL);
    }

    @Override
    public CompletableFuture<MessageAckStatus> nack() {
      natsMessage.nak();
      return CompletableFuture.completedFuture(MessageAckStatus.SUCCESSFUL);
    }

    @Override
    public String toString() {
      return "NatsMessageContainer{" + "natsMessage=" + natsMessage + '}';
    }
  }

  @Override
  public void close() {
    try {
      natsConnection.close();
      pullExecutorService.shutdown();
    } catch (InterruptedException e) {
      throw new NatsSubscriberException("Problem when closing connection", e);
    }
  }
}

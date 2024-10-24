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

import com.dremio.common.util.Closeable;
import com.dremio.services.pubsub.MessageConsumer;
import com.dremio.services.pubsub.MessagePublisher;
import com.dremio.services.pubsub.MessagePublisherOptions;
import com.dremio.services.pubsub.MessageSubscriber;
import com.dremio.services.pubsub.MessageSubscriberOptions;
import com.dremio.services.pubsub.PubSubClient;
import com.dremio.services.pubsub.Subscription;
import com.dremio.services.pubsub.Topic;
import com.dremio.services.pubsub.nats.management.NatsStreamManager;
import com.dremio.services.pubsub.nats.management.StreamManager;
import com.dremio.services.pubsub.nats.reflection.ClassToInstanceUtil;
import com.google.protobuf.Message;

public class NatsPubSubClient implements PubSubClient, Closeable {

  private final String natsServerUrl;

  /**
   * When running within a Kubernetes cluster, you should use the service name dremio-nats instead
   * of localhost because the service name is resolved by Kubernetes DNS to the IP address of the
   * service. This allows different pods within the cluster to communicate with each other using
   * service names.
   *
   * <p>kubectl get service returns: dremio-nats ClusterIP 10.96.42.199 <none> 4222/TCP
   */
  // TODO(DX-94555): inject the URL from the environment
  public NatsPubSubClient() {
    this("nats://dremio-nats:4222");
  }

  public NatsPubSubClient(String natsServerUrl) {
    this.natsServerUrl = natsServerUrl;
  }

  @Override
  public void close() {}

  /**
   * A dedicated MessagePublisher for a given subject. Each subject should have own instance of the
   * MessagePublisher.
   */
  @Override
  public <M extends Message> MessagePublisher<M> getPublisher(
      Class<? extends Topic<M>> topicClass, MessagePublisherOptions options) {
    Topic<M> subject = ClassToInstanceUtil.toTopicInstance(topicClass);

    String subjectName = subject.getName();

    /**
     * The code below assumes that the stream and subjects are already created.
     *
     * <p>Also, note that If no explicit subject is specified, the default subject will be the same
     * name as the stream.
     */
    NatsPublisher<M> messageNatsPublisher = new NatsPublisher<>(subjectName, natsServerUrl);
    messageNatsPublisher.connect();
    return messageNatsPublisher;
  }

  /**
   * A dedicated MessageSubscriber for a given subject. Each subject should have own instance of the
   * MessageSubscriber.
   */
  @Override
  public <M extends Message> MessageSubscriber<M> getSubscriber(
      Class<? extends Subscription<M>> subscriptionClass,
      MessageConsumer<M> messageConsumer,
      MessageSubscriberOptions options) {
    Subscription<M> subscription = ClassToInstanceUtil.toSubscriptionInstance(subscriptionClass);
    Topic<M> subject = ClassToInstanceUtil.toTopicInstance(subscription.getTopicClass());

    NatsSubscriber<M> natsSubscriber =
        new NatsSubscriber<>(subject, subscription, natsServerUrl, messageConsumer, options);
    natsSubscriber.connect();
    return natsSubscriber;
  }

  /**
   * A single StreamManager instance can be used for managing multiple Streams. You can safely share
   * the instance of this class.
   */
  public StreamManager getStreamManager() {
    NatsStreamManager natsStreamManager = new NatsStreamManager(natsServerUrl);
    natsStreamManager.connect();
    return natsStreamManager;
  }
}

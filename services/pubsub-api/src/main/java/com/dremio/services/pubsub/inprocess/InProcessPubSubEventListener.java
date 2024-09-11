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

import javax.annotation.Nullable;

/** Listener for events in {@link InProcessPubSubClient}. */
public interface InProcessPubSubEventListener {
  /**
   * Called when a message is published or failed to publish.
   *
   * @param topicName Name of the topic.
   * @param queueLength Number of items in the topic queue.
   * @param success Whether the publish was successful or not.
   * @param exceptionName Name of the exception class.
   */
  void onPublish(
      String topicName, int queueLength, boolean success, @Nullable String exceptionName);

  /**
   * Called when a message is received.
   *
   * @param topicName Name of the topic.
   * @param subscriptionName Name of the subscription.
   * @param success Whether the message was processed w/o an exception.
   * @param exceptionName Name of the exception class.
   */
  void onMessageReceived(
      String topicName, String subscriptionName, boolean success, @Nullable String exceptionName);
}

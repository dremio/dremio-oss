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
package com.dremio.services.pubsub;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import org.immutables.value.Value;

@Value.Immutable
public interface MessageSubscriberOptions {

  /**
   * The maxAckPending setting specifies the maximum number of unacknowledged messages that a
   * consumer can have at any given time. Once this limit is reached, the server will stop
   * delivering new messages to the consumer until some of the outstanding messages are
   * acknowledged. See the MaxAckPending
   * (https://docs.nats.io/nats-concepts/jetstream/consumers#maxackpending)
   *
   * <p>If not set, the default will be set to 1000.
   *
   * <p>It allows the same behaviour as GCP Pub-Sub max_outstanding_elements_count The one
   * difference is that for GCP, max_outstanding_elements_count is per subscriber, and here, it
   * works across all subscriptions bound to a consumer. So, the GCP value must be multiplied by the
   * number of expected NATS consumers.
   */
  OptionalLong maxAckPending();

  /**
   * The AckWait setting determines the amount of time the server will wait for an acknowledgment
   * for a delivered message before considering it as not acknowledged (nack) and redelivering it.
   * This is particularly useful in ensuring that messages are not lost and can be redelivered if
   * the consumer fails to acknowledge them within the specified timeframe.
   *
   * <p>If not set, the default will be set to 10 seconds
   *
   * <p>For the pub-sub, see https://cloud.google.com/pubsub/docs/lease-management
   */
  Optional<Duration> ackWait();
}

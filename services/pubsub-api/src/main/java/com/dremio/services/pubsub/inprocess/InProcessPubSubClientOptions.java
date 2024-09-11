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

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

/** Options for {@link InProcessPubSubClient}. */
@Options
public final class InProcessPubSubClientOptions {
  /** Parallelism of the executor, must be 2 or greater. */
  public static final TypeValidators.LongValidator PARALLELISM =
      new TypeValidators.LongValidator("dremio.pubsub.inprocess.parallelism", 2);

  /** How often to poll from the message queue in absence of any publishing events. */
  public static final TypeValidators.LongValidator QUEUE_POLL_MILLIS =
      new TypeValidators.LongValidator("dremio.pubsub.inprocess.queue_poll_millis", 10);

  /** How long to wait for executor service to terminate. */
  public static final TypeValidators.LongValidator TERMINATION_TIMEOUT_MILLIS =
      new TypeValidators.LongValidator("dremio.pubsub.inprocess.termination_timeout_millis", 100);

  /**
   * Maximum number of messages to poll in one pass. If nothing is published, with the queue poll
   * interval this defines the maximum rate of processing.
   */
  public static final TypeValidators.LongValidator MAX_MESSAGES_TO_POLL =
      new TypeValidators.LongValidator("dremio.pubsub.inprocess.max_messages_to_poll", 50);

  /** Maximum number of messages that could be processed/queued to executor at most. */
  public static final TypeValidators.LongValidator MAX_MESSAGES_IN_PROCESSING =
      new TypeValidators.LongValidator("dremio.pubsub.inprocess.max_messages_in_processing", 50);

  /** Maximum number of redelivery attempts. */
  public static final TypeValidators.LongValidator MAX_REDELIVERY_ATTEMPTS =
      new TypeValidators.LongValidator("dremio.pubsub.inprocess.max_redelivery_attempts", 5);

  /** Maximum number of messages in a topic queue before it starts blocking publishing. */
  public static final TypeValidators.LongValidator MAX_MESSAGES_IN_QUEUE =
      new TypeValidators.LongValidator("dremio.pubsub.inprocess.max_messages_in_queue", 10_000);

  /** Maximum number of messages in the redelivery queue before it starts blocking. */
  public static final TypeValidators.LongValidator MAX_REDELIVERY_MESSAGES =
      new TypeValidators.LongValidator("dremio.pubsub.inprocess.max_redelivery_messages", 1_000);

  /** Minimum delay in seconds for redelivery. */
  public static final TypeValidators.LongValidator MIN_DELAY_FOR_REDELIVERY_SECONDS =
      new TypeValidators.LongValidator(
          "dremio.pubsub.inprocess.min_delay_for_redelivery_seconds", 10);

  /** Maximum delay in seconds for redelivery. */
  public static final TypeValidators.LongValidator MAX_DELAY_FOR_REDELIVERY_SECONDS =
      new TypeValidators.LongValidator(
          "dremio.pubsub.inprocess.max_delay_for_redelivery_seconds", 60);
}

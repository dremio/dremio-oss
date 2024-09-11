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
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.immutables.value.Value;

@Value.Immutable
public interface MessagePublisherOptions {
  /**
   * Whether to accumulate messages on the client before sending them in batch to the server. This
   * behavior is controlled by:
   * <li>Batch size is how many records to accumulate at most before sending the batch.
   * <li>How much data to accumulate in bytes before it triggers the send to the server.
   * <li>How long to wait to accumulate the batch size or the data bytes before sending.
   */
  boolean enableBatching();

  /** How many messages to send together. */
  OptionalInt batchSize();

  /** How many bytes to accumulate before sending the batched messages. */
  OptionalLong batchRequestSizeBytes();

  /** How long to wait before sending not full batches. */
  Optional<Duration> batchDelay();
}

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
package com.dremio.sabot.op.receiver;

import java.io.IOException;

/**
 * A batch buffer is responsible for queuing incoming batches until a consumer is ready to receive them. It will also
 * inform upstream if the batch cannot be accepted.
 */
public interface RawBatchBuffer extends RawFragmentBatchProvider {

  /**
   * Add the next new raw fragment batch to the buffer.
   *
   * @param batch
   *          Batch to enqueue
   * @throws IOException
   * @return Whether response should be returned.
   */
  void enqueue(RawFragmentBatch batch);

  void streamComplete();

}

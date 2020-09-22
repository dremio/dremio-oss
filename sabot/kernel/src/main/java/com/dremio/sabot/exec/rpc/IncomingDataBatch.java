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
package com.dremio.sabot.exec.rpc;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.LargeMemoryUtil;

import com.dremio.common.memory.AllocatorUtil;
import com.dremio.exec.proto.ExecRPC.FragmentRecordBatch;
import com.dremio.sabot.op.receiver.RawFragmentBatch;
import com.google.common.base.Preconditions;

/**
 * An incoming batch of data. The data is held by the original allocator. Any use of the associated data must be
 * leveraged through the use of newRawFragmentBatch().
 */
public class IncomingDataBatch {

  private final FragmentRecordBatch header;
  private final ArrowBuf body;
  private final AckSender sender;

  /**
   * Create a new batch. Does not impact reference counts of body.
   *
   * @param header
   *          Batch header
   * @param body
   *          Data body. Could be null.
   * @param sender
   *          AckSender to use for underlying RawFragmentBatches.
   */
  public IncomingDataBatch(FragmentRecordBatch header, ArrowBuf body, AckSender sender) {
    Preconditions.checkNotNull(header);
    Preconditions.checkNotNull(sender);
    this.header = header;
    this.body = body;
    this.sender = sender;
  }

  /**
   * Create a new RawFragmentBatch based on this incoming data batch that is transferred into the provided allocator.
   * Also increments the AckSender to expect one additional return message.
   *
   * @param allocator
   *          Target allocator that should be associated with data underlying this batch.
   * @return The newly created RawFragmentBatch
   */
  public RawFragmentBatch newRawFragmentBatch(final BufferAllocator allocator) {
    final ArrowBuf transferredBuffer = body == null ? null : body.getReferenceManager()
      .transferOwnership(body, allocator)
      .getTransferredBuffer();
    sender.increment();
    return new RawFragmentBatch(header, transferredBuffer, sender);
  }

  public FragmentRecordBatch getHeader() {
    return header;
  }

  /**
   * Check if the batch size is acceptable.
   * throws exception if not acceptable.
   */
  public void checkAcceptance(final BufferAllocator allocator) {
    AllocatorUtil.ensureHeadroom(allocator, size());
    sender.increment();
    sender.sendOk();
  }

  public int size() {
    if (body == null){
      return 0;
    }

    return LargeMemoryUtil.checkedCastToInt(body.getPossibleMemoryConsumed());
  }
}

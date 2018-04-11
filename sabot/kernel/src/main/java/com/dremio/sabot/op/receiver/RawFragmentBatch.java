/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.concurrent.atomic.AtomicBoolean;

import com.dremio.exec.proto.ExecRPC.FragmentRecordBatch;
import com.dremio.sabot.exec.rpc.AckSender;

import io.netty.buffer.ArrowBuf;

public class RawFragmentBatch implements AutoCloseable {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RawFragmentBatch.class);

  private final FragmentRecordBatch header;
  private final ArrowBuf body;
  private final AckSender sender;
  private final AtomicBoolean ackSent = new AtomicBoolean(false);

  public RawFragmentBatch(FragmentRecordBatch header, ArrowBuf body, AckSender sender) {
    this.header = header;
    this.sender = sender;
    this.body = body;
    if (body != null) {
      body.retain(1);
    }
    // ACK has been sent when the batch was spilled
    ackSent.set(sender == null);
  }

  public FragmentRecordBatch getHeader() {
    return header;
  }

  public ArrowBuf getBody() {
    return body;
  }

  @Override
  public String toString() {
    return "RawFragmentBatch [header=" + header + ", body=" + body + "]";
  }

  public void close() {
    if (body != null) {
      body.release();
    }
  }

  public AckSender getSender() {
    return sender;
  }

  public synchronized void sendOk() {
    if (sender != null && ackSent.compareAndSet(false, true)) {
      sender.sendOk();
    }
  }

  public long getByteCount() {
    return body == null ? 0 : body.readableBytes();
  }

  public boolean isAckSent() {
    return ackSent.get();
  }
}

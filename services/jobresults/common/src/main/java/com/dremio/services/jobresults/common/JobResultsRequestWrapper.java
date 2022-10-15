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
package com.dremio.services.jobresults.common;

import java.util.Arrays;

import org.apache.arrow.memory.util.LargeMemoryUtil;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.base.Objects;

import io.netty.buffer.ByteBuf;

/**
 * Wrapper around outgoing JobResultsRequest from grpc client side.
 * Its similar to JobResultsRequest except JobResultsRequest.data is not on heap memory but on direct memory
 * using arrow/netty buffers.
 */
public class JobResultsRequestWrapper implements AutoCloseable {
  private final UserBitShared.QueryData header;
  private final long sequenceId;
  private final ByteBuf[] byteBuffers;
  private final CoordinationProtos.NodeEndpoint foreman;

  public JobResultsRequestWrapper(UserBitShared.QueryData header,
                                  long sequenceId,
                                  ByteBuf[] byteBuffers,
                                  CoordinationProtos.NodeEndpoint foreman) {
    this.header = header;
    this.sequenceId = sequenceId;
    this.byteBuffers = byteBuffers;
    this.foreman = foreman;
  }

  public UserBitShared.QueryData getHeader() {
    return header;
  }

  public long getSequenceId() {
    return sequenceId;
  }

  public ByteBuf[] getByteBuffers() {
    return byteBuffers;
  }

  public CoordinationProtos.NodeEndpoint getForeman() {
    return foreman;
  }

  public int getByteBuffersLength() {
    if (byteBuffers == null || byteBuffers.length == 0) {
      return 0;
    }
    int length = 0;
    for (ByteBuf byteBuf: byteBuffers) {
      length += byteBuf == null ? 0 : LargeMemoryUtil.checkedCastToInt(byteBuf.readableBytes());
    }
    return length;
  }

  @Override
  // This is not performant enough. Does deep comparison
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }
    JobResultsRequestWrapper wrapper = (JobResultsRequestWrapper) o;
    return sequenceId == wrapper.sequenceId &&
      Objects.equal(header, wrapper.header) &&
      Arrays.equals(toByteArray(byteBuffers), toByteArray(wrapper.byteBuffers)) &&
      Objects.equal(foreman, wrapper.foreman);
  }

  private byte[] toByteArray(ByteBuf[] a) {
    if (a == null || a.length == 0) {
      return new byte[0];
    }
    int totalBytes = Arrays.stream(a).map(x->x.readableBytes()).reduce (0, (x,y)->x+y);
    byte[] byteArray = new byte[totalBytes];
    int index = 0;
    for (ByteBuf b: a) {
      int len = b.readableBytes();
      for (int i=0; i < len; i++, index++) {
        byteArray[index] = b.getByte(i);
      }
    }
    return byteArray;
  }

  @Override
  // This should not be used. Implemented for avoiding checkstyle/errorprone errors.
  public int hashCode() {
    return Objects.hashCode(header, sequenceId, Arrays.hashCode(byteBuffers), foreman);
  }

  @Override
  public void close() {
    if (byteBuffers != null) {
      Arrays.stream(byteBuffers).filter(buf -> buf !=null).forEach(b -> b.release());
    }
  }
}

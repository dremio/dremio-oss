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
package com.dremio.common.utils.protos;

import java.util.Arrays;


import com.dremio.exec.proto.UserBitShared.QueryData;
import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBuf;

public class QueryWritableBatch {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWritableBatch.class);

  private final QueryData header;
  private final ByteBuf[] buffers;

  public QueryWritableBatch(QueryData header, ByteBuf... buffers) {
    this.header = header;
    this.buffers = buffers;
    for(ByteBuf b : buffers){
      Preconditions.checkNotNull(b);
    }
  }

  public ByteBuf[] getBuffers() {
    return buffers;
  }

  public long getByteCount() {
    long n = 0;
    for (ByteBuf buf : buffers) {
      n += buf.readableBytes();
    }
    return n;
  }

  public QueryData getHeader() {
    return header;
  }

  @Override
  public String toString() {
    return "QueryWritableBatch [header=" + header + ", buffers=" + Arrays.toString(buffers) + "]";
  }
}

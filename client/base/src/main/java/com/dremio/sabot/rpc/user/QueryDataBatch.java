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
package com.dremio.sabot.rpc.user;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.exec.proto.UserBitShared.QueryData;

public class QueryDataBatch implements AutoCloseable {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryDataBatch.class);

  private final QueryData header;
  private final ArrowBuf data;
  private final AtomicBoolean released = new AtomicBoolean(false);

  public QueryDataBatch(QueryData header, ArrowBuf data) {
    // logger.debug("New Result Batch with header {} and data {}", header, data);
    this.header = header;
    this.data = data;
    if (this.data != null) {
      data.retain(1);
    }
  }

  public QueryData getHeader() {
    return header;
  }

  public ArrowBuf getData() {
    return data;
  }

  public boolean hasData() {
    return data != null;
  }

  public void release() {
    if (!released.compareAndSet(false, true)) {
      throw new IllegalStateException("QueryDataBatch was released twice.");
    }

    if (data != null) {
      data.release(1);
    }
  }

  @Override
  public String toString() {
    return "QueryResultBatch [header=" + header + ", data=" + data + "]";
  }

  @Override
  public void close() {
    release();
  }


}

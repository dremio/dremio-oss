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
package com.dremio.common;

import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;

/**
 * Class that wraps a bytebuf to make it closeable.
 */
public class CloseableByteBuf implements AutoCloseable {
  private final ByteBuf buf;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public CloseableByteBuf(ByteBuf buf) {
    super();
    this.buf = buf;
  }

  @Override
  public void close() throws Exception {
    if(closed.compareAndSet(false, true)){
      buf.release();
    }
  }

}

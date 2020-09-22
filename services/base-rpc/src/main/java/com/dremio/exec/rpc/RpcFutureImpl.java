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
package com.dremio.exec.rpc;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ForwardingListenableFuture;

import io.netty.buffer.ByteBuf;

class RpcFutureImpl<V> extends ForwardingListenableFuture.SimpleForwardingListenableFuture<V>
  implements RpcFuture<V>, RpcOutcomeListener<V>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RpcFutureImpl.class);

  private volatile ByteBuf buffer;

  public RpcFutureImpl() {
    super(new InnerFuture<V>());
  }

  private static class InnerFuture<T> extends AbstractFuture<T> {
    // we rewrite these so that the parent can see them

    void setValue(T value) {
      super.set(value);
    }

    @Override
    protected boolean setException(Throwable t) {
      return super.setException(t);
    }
  }

  @Override
  public void failed(RpcException ex) {
    ( (InnerFuture<V>)delegate()).setException(ex);
  }

  @Override
  public void success(V value, ByteBuf buffer) {
    this.buffer = buffer;
    ( (InnerFuture<V>)delegate()).setValue(value);
  }

  @Override
  public void interrupted(final InterruptedException ex) {
    // Propagate the interrupt to inner future
    ( (InnerFuture<V>)delegate()).cancel(true);
  }

  @Override
  public ByteBuf getBuffer() {
    return buffer;
  }

  public void release() {
    if (buffer != null) {
      buffer.release();
    }
  }

}

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
package com.dremio.exec.work.user;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.CloseableByteBuf;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

public class LocalUserUtil {

  public static QueryDataBatch acquireData(BufferAllocator allocator, RpcOutcomeListener<Ack> listener, QueryWritableBatch result){
    ArrowBuf out = null;
    final ByteBuf[] buffers = result.getBuffers();

    final CloseableListener closeableListener = new CloseableListener(listener);
    try(
        // need to make sure that the listener is informed after we release the buffers from the incoming QueryWritableBatch.
        final CloseableListener autoClosed = closeableListener;
        final CloseableBuffers closeable = new CloseableBuffers(buffers);
        ){

      // consolidate data in a single buffer since that is what we need
      // downstream right now. (we can fix this since we know the slices are
      // right through some kind of composite buffer for record batch loader.
      if(buffers != null && buffers.length > 0){
        int length = 0;
        for(int i =0; i < buffers.length; i++){
          length += buffers[i].readableBytes();
        }
        out = allocator.buffer(length);
        for(int i =0; i < buffers.length; i++){
          ByteBuffer src = buffers[i].nioBuffer();
          int srcLength = src.remaining();
          out.setBytes(out.writerIndex(), src);
          out.writerIndex(srcLength + out.writerIndex());
        }
      }else{
        out = allocator.buffer(0);
      }

      try(ArrowBuf ensureClosed = out) {
        final QueryDataBatch batch = new QueryDataBatch(result.getHeader(), out);
        return batch;
      }

    } catch(Exception ex) {
      closeableListener.setFailure(new RpcException(ex));
      throw Throwables.propagate(ex);
    }

  }

  private static class CloseableListener implements AutoCloseable {

    private final RpcOutcomeListener<Ack> listener;
    private RpcException ex;

    private void setFailure(RpcException ex){
      this.ex = ex;
    }

    public CloseableListener(RpcOutcomeListener<Ack> listener){
      this.listener = listener;
    }

    @Override
    public void close() {
      if(ex == null){
        listener.success(Acks.OK, null);
      } else{
        listener.failed(ex);
      }
    }

  }

  private static class CloseableBuffers implements AutoCloseable {

    final ByteBuf[] buffers;

    public CloseableBuffers(ByteBuf[] buffers) {
      super();
      this.buffers = buffers;
    }

    @Override
    public void close() {
      try{
        AutoCloseables.close(Arrays.stream(buffers)
          .map(CloseableByteBuf::new)
          .collect(ImmutableList.toImmutableList()));
      } catch(Exception ex){
        throw new RuntimeException(ex);
      }
    }
  }
}


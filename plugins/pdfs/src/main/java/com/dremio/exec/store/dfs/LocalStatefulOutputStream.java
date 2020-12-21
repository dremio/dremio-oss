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
package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.util.concurrent.DremioFutures;
import com.dremio.exec.dfs.proto.DFS.RpcType;
import com.dremio.exec.dfs.proto.DFS.WriteDataRequest;
import com.dremio.exec.dfs.proto.DFS.WriteDataResponse;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

/**
 * An OutputStream that buffers the stream locally, flushing to the remote node
 * on occasion. The remote stream is stateful and expects the local stream to
 * maintain the remote offset and last update to continue writing.
 *
 * Pieces pulled from harmony.
 */
@NotThreadSafe
class LocalStatefulOutputStream extends OutputStream {

  private static int WRITE_RESPONSE_IN_SECONDS = 10;

  private final FabricCommandRunner runner;
  private final String path;
  private final ByteBuf buf;

  private boolean closed;
  private long remoteOffset;
  private long lastUpdate;

  public LocalStatefulOutputStream(String path, FabricCommandRunner runner, BufferAllocator alloc, int size) {
    this.buf = NettyArrowBuf.unwrapBuffer(alloc.buffer(size));
    this.runner = runner;
    this.path = path;
  }

  @Override
  public void flush() throws IOException {
    flush(false);
  }

  private void flush(boolean evenIfEmpty) throws IOException{
    if(!buf.isReadable() && !evenIfEmpty){
      return;
    }

    final WriteDataRequest req = WriteDataRequest.newBuilder()
      .setPath(path)
      .setLastOffset(remoteOffset)
      .setLastUpdate(lastUpdate)
      .build();

    WriteDataCommand command = new WriteDataCommand(req, buf);
    runner.runCommand(command);
    try{
      WriteDataResponse response = DremioFutures.getChecked(
        command.getFuture(),
        RpcException.class,
        WRITE_RESPONSE_IN_SECONDS,
        TimeUnit.SECONDS,
        RpcException::mapException
      );
      remoteOffset += buf.readableBytes();
      lastUpdate = response.getUpdateTime();
      buf.clear();
    } catch (RpcException | TimeoutException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkNotNull(buffer);
    Preconditions.checkArgument((offset | length) >= 0);
    Preconditions.checkArgument(offset <= buffer.length - length);

    while (length > 0) {
      if (!buf.isWritable()) {
        flush();
      }

      int copyAmount = Math.min(length, buf.writableBytes());
      buf.writeBytes(buffer, offset, copyAmount);
      length -= copyAmount;
      offset += copyAmount;

    }
  }

  @Override
  public synchronized void write(int oneByte) throws IOException {
    if (!buf.isWritable()) {
      flush();
    }
    buf.writeByte(oneByte);
  }

  @Override
  public void close() throws IOException {
    if(closed){
      return;
    }
    try {
      flush(true);
    } finally {
      closed = true;
      buf.release();
    }
  }

  private static class WriteDataCommand extends FutureBitCommand<WriteDataResponse, ProxyConnection> {
    private final ByteBuf buf;
    private final WriteDataRequest request;

    protected WriteDataCommand(WriteDataRequest request, ByteBuf buf) {
      buf.retain();
      this.buf = buf;
      this.request = request;
    }

    @Override
    public void doRpcCall(RpcOutcomeListener<WriteDataResponse> outcomeListener, ProxyConnection connection) {
      connection.send(outcomeListener, RpcType.WRITE_DATA_REQUEST, request, WriteDataResponse.class, buf);
    }
  }
}

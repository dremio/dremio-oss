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

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.ExecRPC;
import com.dremio.exec.proto.FileExec;
import com.dremio.exec.record.FragmentWritableBatch;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tunnel-like API that writes batches/completions to a file. */
public class FileTunnel implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(FileTunnel.class);
  private final FileStreamManager fileStreamManager;
  private final int maxBatchesPerFile;
  private int currentFileSeq;
  private long currentBatchSeq;
  private OutputStream currentOutput;
  private static final int IO_CHUNK_SIZE = 32 * 1024;
  private byte[] ioBuffer;

  public FileTunnel(FileStreamManager fileStreamManager, int maxBatchesPerFile) throws IOException {
    this.fileStreamManager = fileStreamManager;
    this.maxBatchesPerFile = maxBatchesPerFile;
    this.currentOutput = fileStreamManager.createOutputStream(currentFileSeq);
  }

  public long sendRecordBatch(FragmentWritableBatch batch) throws IOException {
    try {
      writeMessage(
          currentOutput,
          FileExec.FileMessage.newBuilder()
              .setType(ExecRPC.RpcType.REQ_RECORD_BATCH)
              .setMsgSeq(currentBatchSeq++)
              .setBodySize(batch.getByteCount())
              .setRecordBatch(batch.getHeader())
              .build(),
          batch.getBuffers());
      if (currentBatchSeq % maxBatchesPerFile == 0) {
        switchOutputStream();
      }
      return currentBatchSeq - 1;
    } finally {
      for (ByteBuf byteBuf : batch.getBuffers()) {
        byteBuf.release();
      }
    }
  }

  public long sendStreamComplete(ExecRPC.FragmentStreamComplete streamComplete) throws IOException {
    logger.debug(
        "sending streamComplete currentFileSeq {} currentBatchSeq {}",
        currentFileSeq,
        currentBatchSeq);
    writeMessage(
        currentOutput,
        FileExec.FileMessage.newBuilder()
            .setType(ExecRPC.RpcType.REQ_STREAM_COMPLETE)
            .setMsgSeq(currentBatchSeq++)
            .setStreamComplete(streamComplete)
            .build(),
        null);
    currentOutput.close();
    currentOutput = null;
    return currentBatchSeq - 1;
  }

  private void switchOutputStream() throws IOException {
    writeMessage(
        currentOutput,
        FileExec.FileMessage.newBuilder()
            // type not set in message to indicate EOF.
            .setMsgSeq(currentBatchSeq++)
            .build(),
        null);

    // close current stream
    currentOutput.close();
    currentOutput = null;

    // create a fresh stream
    ++currentFileSeq;
    currentOutput = fileStreamManager.createOutputStream(currentFileSeq);
  }

  public FileStreamManager getFileStreamManager() {
    return fileStreamManager;
  }

  public int getCurrentFileSeq() {
    return currentFileSeq;
  }

  private void writeMessage(OutputStream output, FileExec.FileMessage message, ByteBuf[] extraBufs)
      throws IOException {
    // msg contains :
    // - protobuf msg
    // - arrow buffers (optional)
    message.writeDelimitedTo(output);
    if (extraBufs != null) {
      for (ByteBuf byteBuf : extraBufs) {
        writeArrowBuf(byteBuf, output);
      }
    }
    output.flush();
  }

  // copy to pre-alloced heap buffer to avoid repeated heap allocations
  private void writeArrowBuf(final ByteBuf buffer, final OutputStream output) throws IOException {
    if (ioBuffer == null) {
      // delay alloc till the first write.
      ioBuffer = new byte[IO_CHUNK_SIZE];
    }
    final int bufferLength = LargeMemoryUtil.checkedCastToInt(buffer.readableBytes());
    for (int writePos = 0; writePos < bufferLength; writePos += ioBuffer.length) {
      final int lengthToWrite = Math.min(ioBuffer.length, bufferLength - writePos);
      buffer.getBytes(writePos, ioBuffer, 0, lengthToWrite);
      output.write(ioBuffer, 0, lengthToWrite);
    }
  }

  @Override
  public void close() throws Exception {
    logger.debug("close fileTunnel");
    AutoCloseables.close(currentOutput);
  }
}

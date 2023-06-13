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
package com.dremio.sabot.op.receiver;

import java.io.IOException;
import java.io.InputStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.cache.VectorAccessibleSerializable;
import com.dremio.exec.proto.FileExec;
import com.dremio.sabot.exec.cursors.FileCursorManager;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactory;
import com.dremio.sabot.exec.rpc.AckSender;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.sabot.threads.sharedres.SharedResourceGroup;
import com.dremio.sabot.threads.sharedres.SharedResourceType;
import com.google.common.base.Preconditions;

public class BatchBufferFromFilesProvider implements RawFragmentBatchProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchBufferFromFilesProvider.class);
  private final FileCursorManager cursorManager;
  private final FileCursorManager.Observer observer;
  private final BufferAllocator allocator;
  private long currentCursor = -1;
  private int currentFileSeq = 0;
  private InputStream currentInputStream;
  private boolean isStreamDone;
  private final String resourceName;

  public BatchBufferFromFilesProvider(String uniqueId,
                                      int readerMajorFragId,
                                      SharedResourceGroup resourceGroup,
                                      BufferAllocator parentAllocator,
                                      FileCursorManagerFactory cursorManagerFactory) {
    this.cursorManager = cursorManagerFactory.getManager(uniqueId);

    this.resourceName = "reader-" + readerMajorFragId + "-file-" + uniqueId;
    final SharedResource resource = resourceGroup.createResource(this.resourceName,
      SharedResourceType.SEND_MSG_DATA);
    this.observer = cursorManager.registerReader(resource);
    this.allocator = parentAllocator.newChildAllocator("bridgeFileReader", 0, Long.MAX_VALUE);
  }

  @Override
  public RawFragmentBatch getNext() {
    try {
      while (true) {
        if (currentCursor == cursorManager.getWriteCursor()) {
          // caught up with writer, nothing more to read.
          return null;
        }

        RawFragmentBatch batch = getNextMessageFromStream();
        if (batch != null || isStreamDone) {
          return batch;
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private RawFragmentBatch getNextMessageFromStream() throws IOException {
    if (currentInputStream == null) {
      currentInputStream = cursorManager.getFileStreamManager().getInputStream(currentFileSeq);
    }

    FileExec.FileMessage fileMessage = FileExec.FileMessage.parseDelimitedFrom(currentInputStream);
    if (fileMessage == null) {
      String message = "Failure reading next batch of data from file - " + this.resourceName + ".";
      logger.error(message);
      throw new IOException(message);
    }
    Preconditions.checkState(fileMessage.hasMsgSeq(), "invalid msg found in file");
    if (!fileMessage.hasType()) {
      // indicates end-of-file, switch to next file
      currentInputStream.close();
      updateCursor(currentFileSeq, fileMessage.getMsgSeq());

      ++currentFileSeq;
      currentInputStream = cursorManager.getFileStreamManager().getInputStream(currentFileSeq);
      return null;
    }
    if (fileMessage.hasStreamComplete()) {
      updateCursor(currentFileSeq, fileMessage.getMsgSeq());
      isStreamDone = true;
      return null;
    }

    long bodySize = fileMessage.getBodySize();
    try (ArrowBuf body = allocator.buffer(bodySize)) {
      // read the body
      VectorAccessibleSerializable.readIntoArrowBuf(currentInputStream, body, bodySize);

      updateCursor(currentFileSeq, fileMessage.getMsgSeq());
      return new RawFragmentBatch(fileMessage.getRecordBatch(), body, AckSender.NO_OP);
    }
  }

  @Override
  public boolean isStreamDone() {
    return isStreamDone;
  }

  private void updateCursor(int fileSeq, long updatedCursor) {
    Preconditions.checkState(updatedCursor >= currentCursor,
      "cursor value should not reduce, currentCursor " + currentCursor + " updatedCursor " + updatedCursor);
    currentCursor = updatedCursor;
    observer.updateCursor(fileSeq, updatedCursor);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(observer, currentInputStream, allocator);
  }
}

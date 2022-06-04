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

import java.io.IOException;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.ExecRPC;
import com.dremio.exec.record.FragmentWritableBatch;
import com.dremio.sabot.exec.cursors.FileCursorManager;
import com.dremio.sabot.exec.cursors.FileCursorManagerFactory;
import com.dremio.sabot.threads.sharedres.SharedResource;

/**
 * Wrapper over FileTunnel to account pending batches and flow control (TODO)
 */
public class AccountingFileTunnel implements AutoCloseable {
  private final FileTunnel tunnel;
  private final FileCursorManager.Observer observer;
  private boolean allReceiversDone;

  public AccountingFileTunnel(FileTunnel tunnel, FileCursorManagerFactory cursorManagerFactory, SharedResource sharedResource) {
    this.tunnel = tunnel;

    FileStreamManager streamManager = tunnel.getFileStreamManager();
    this.observer = cursorManagerFactory.getManager(streamManager.getId()).registerWriter(
      streamManager,
      sharedResource,
      () -> allReceiversDone = true);
  }

  public void sendStreamComplete(ExecRPC.FragmentStreamComplete streamComplete) throws IOException {
    long cursor = tunnel.sendStreamComplete(streamComplete);
    observer.updateCursor(tunnel.getCurrentFileSeq(), cursor);
  }

  public void sendRecordBatch(FragmentWritableBatch batch) throws IOException {
    long cursor = tunnel.sendRecordBatch(batch);
    observer.updateCursor(tunnel.getCurrentFileSeq(), cursor);
  }

  public boolean isAllReceiversDone() {
    return allReceiversDone;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(tunnel, observer);
  }
}

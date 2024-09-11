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

/**
 * Implementation of the HistoryEventHandler interface that provides a no-op (no operation)
 * implementation for all its methods. Instantiated if HistoryEventHandler implementation is not
 * available to prevent repeated null checks within IcebergCommitOpHelper.
 */
public class NoopHistoryEventHandler implements HistoryEventHandler {

  @Override
  public void commit() throws Exception {}

  @Override
  public void process(FileLoadInfo fileLoadInfo) throws Exception {}

  @Override
  public void revert(String queryId, Exception ex) {}

  @Override
  public void close() throws Exception {}
}

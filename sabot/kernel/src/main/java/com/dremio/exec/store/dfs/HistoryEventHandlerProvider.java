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

import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.physical.config.copyinto.CopyIntoHistoryWriterCommitterPOP;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * A factory class that provides methods for creating specific implementations of the {@link
 * HistoryEventHandler} interface based on the input configuration.
 */
public final class HistoryEventHandlerProvider {

  private HistoryEventHandlerProvider() {}

  /**
   * Returns an instance of {@link HistoryEventHandler} based on the provided configuration.
   *
   * @param context The operator context associated with the operation.
   * @param config The configuration object.
   * @param <T> The type of WriterCommitterPOP.
   * @return An instance of {@link HistoryEventHandler} based on the configuration.
   * @throws UnsupportedOperationException if the configuration type is unrecognized.
   */
  public static <T extends WriterCommitterPOP> HistoryEventHandler get(
      OperatorContext context, T config) {
    if (config instanceof CopyIntoHistoryWriterCommitterPOP) {
      return ((CopyIntoHistoryWriterCommitterPOP) config).getHistoryEventHandler(context);
    }
    throw new UnsupportedOperationException(
        "Unrecognized config type, unable to instantiate HistoryEventHandler");
  }
}

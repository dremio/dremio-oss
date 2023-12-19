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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.physical.config.copyinto.CopyIntoErrorWriterCommitterPOP;
import com.dremio.exec.store.dfs.copyinto.CopyIntoErrorsHandler;
import com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePlugin;
import com.dremio.sabot.exec.context.OperatorContext;

public class TestErrorHandlerProvider {

  @Test
  public void testGetErrorHandlerWithCopyIntoErrorWriterCommitterPOP() {
    CopyIntoErrorWriterCommitterPOP config = mock(CopyIntoErrorWriterCommitterPOP.class);
    when(config.getSystemIcebergTablesStoragePlugin()).thenReturn(mock(SystemIcebergTablesStoragePlugin.class));
    OperatorContext context = mock(OperatorContext.class);
    when(config.getErrorHandler(context)).thenReturn(mock(CopyIntoErrorsHandler.class));
    ErrorHandler errorHandler = ErrorHandlerProvider.getErrorHandler(context, config);

    assertThat(errorHandler).isInstanceOf(CopyIntoErrorsHandler.class);
  }

  @Test
  public void testGetErrorHandlerWithUnrecognizedConfigType() {
    WriterCommitterPOP config = mock(WriterCommitterPOP.class);
    OperatorContext context = mock(OperatorContext.class);
    assertThrows(UnsupportedOperationException.class, () -> {
      ErrorHandlerProvider.getErrorHandler(context, config);
    });
  }
}

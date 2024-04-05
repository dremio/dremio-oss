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
package com.dremio.exec.store.iceberg.nessie;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestIcebergNessieCommand {
  private static final IcebergTableIdentifier ID = new IcebergNessieTableIdentifier("ns", "ptr");

  private final Configuration conf = new Configuration();

  @Test
  void testDeleteTableFailure() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    DremioFileIO io = Mockito.mock(DremioFileIO.class);
    Mockito.when(io.getFs()).thenReturn(fs);
    IcebergNessieTableOperations ops = Mockito.mock(IcebergNessieTableOperations.class);
    Mockito.when(ops.io()).thenReturn(io);
    IcebergNessieCommand command = new IcebergNessieCommand(ID, conf, ops, null);
    Mockito.when(fs.delete(any(), anyBoolean()))
        .thenThrow(new RuntimeException("test-exception-1"));
    assertThatThrownBy(command::deleteTable)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("test-exception-1");
    Mockito.verify(ops, Mockito.times(1)).deleteKey();
  }

  @Test
  void testDoubleFailure() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    DremioFileIO io = Mockito.mock(DremioFileIO.class);
    Mockito.when(io.getFs()).thenReturn(fs);
    IcebergNessieTableOperations ops = Mockito.mock(IcebergNessieTableOperations.class);
    Mockito.when(ops.io()).thenReturn(io);
    IcebergNessieCommand command = new IcebergNessieCommand(ID, conf, ops, null);
    RuntimeException innerException = new RuntimeException("test-exception-1");
    Mockito.doThrow(innerException).when(ops).deleteKey();
    Mockito.when(fs.delete(any(), anyBoolean()))
        .thenThrow(new RuntimeException("test-exception-2"));
    assertThatThrownBy(command::deleteTable)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("test-exception-2")
        .hasSuppressedException(innerException);
  }
}

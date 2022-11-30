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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;

class TestIcebergNessieCommand {
  private static final IcebergTableIdentifier ID = new IcebergNessieTableIdentifier("ns", "ptr");

  @Test
  void testDeleteTableFailure() {
    MutablePlugin plugin = Mockito.mock(MutablePlugin.class);
    IcebergNessieTableOperations ops = Mockito.mock(IcebergNessieTableOperations.class);
    IcebergNessieCommand command = new IcebergNessieCommand(ID, null, null, ops, plugin);
    Mockito.when(plugin.getFsConfCopy()).thenThrow(new RuntimeException("test-exception-1"));
    assertThatThrownBy(command::deleteTable).isInstanceOf(RuntimeException.class).hasMessage("test-exception-1");
    Mockito.verify(ops, Mockito.times(1)).deleteKey();
  }

  @Test
  void testDoubleFailure() {
    MutablePlugin plugin = Mockito.mock(MutablePlugin.class);
    IcebergNessieTableOperations ops = Mockito.mock(IcebergNessieTableOperations.class);
    IcebergNessieCommand command = new IcebergNessieCommand(ID, null, null, ops, plugin);
    RuntimeException innerException = new RuntimeException("test-exception-1");
    Mockito.doThrow(innerException).when(ops).deleteKey();
    Mockito.when(plugin.getFsConfCopy()).thenThrow(new RuntimeException("test-exception-2"));
    assertThatThrownBy(command::deleteTable)
      .isInstanceOf(RuntimeException.class)
      .hasMessage("test-exception-2")
      .hasSuppressedException(innerException);
  }

}

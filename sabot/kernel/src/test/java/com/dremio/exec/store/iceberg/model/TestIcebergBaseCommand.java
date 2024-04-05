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
package com.dremio.exec.store.iceberg.model;

import com.dremio.BaseTestQuery;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableOperations;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class TestIcebergBaseCommand extends BaseTestQuery {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private final TableOperations tableOperations = Mockito.mock(TableOperations.class);

  @Test
  public void testMissingManifestOnLoadTable() {
    File tableFolder = new File(folder.getRoot(), "testMissingManifest");
    MockCommand cmd = new MockCommand(tableFolder);
    Assertions.assertThatThrownBy(cmd::loadTable).hasMessageContaining("testMissingManifest");
  }

  @Test
  public void testMissingRootPointer() {
    File tableFolder = new File(folder.getRoot(), "testMissingRootPointer");
    MockCommand cmd = new MockCommand(tableFolder);
    Assertions.assertThatThrownBy(cmd::getRootPointer)
        .hasMessageContaining("testMissingRootPointer");
  }

  private class MockCommand extends IcebergBaseCommand {
    public MockCommand(File tableFolder) {
      super(
          new Configuration(),
          tableFolder.getAbsolutePath(),
          TestIcebergBaseCommand.this.tableOperations,
          null);
    }
  }
}

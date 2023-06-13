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

package com.dremio.exec.store.deltalake;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;

public class TestDeltaLakeFormatDatasetAccessor {

  @Test
  public void testMetadataStaleCheckNoSignature() throws IOException {
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    FileSelection selection = FileSelection.create(fs, Path.of(new File("dummy").getAbsolutePath()));
    BytesOutput signature = BytesOutput.NONE;
    DatasetType dt = DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
    FileSystemPlugin fileSystemPlugin = mock(FileSystemPlugin.class);
    DeltaLakeFormatPlugin deltaLakeFormatPlugin = mock(DeltaLakeFormatPlugin.class);
    NamespaceKey key = new NamespaceKey("dummy");
    DeltaLakeFormatDatasetAccessor deltaLakeFormatDatasetAccessor = new DeltaLakeFormatDatasetAccessor(dt, fs, fileSystemPlugin, selection, key, deltaLakeFormatPlugin);

    // when there is no read signature, metadataValid should return false
    assertFalse(deltaLakeFormatDatasetAccessor.metadataValid(signature, deltaLakeFormatDatasetAccessor, mock(DatasetMetadata.class), fs));
  }
}

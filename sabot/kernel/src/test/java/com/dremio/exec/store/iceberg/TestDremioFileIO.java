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

package com.dremio.exec.store.iceberg;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.attribute.FileTime;

import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;

public class TestDremioFileIO {

  @Test
  public void testFileVersion() throws Exception {
    FileSystemPlugin fileSystemPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    FileSystem fs = mock(FileSystem.class);
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "local");
    DremioFileIO io = new DremioFileIO(fs, conf,  fileSystemPlugin);
    FileAttributes fileAttributes = mock(FileAttributes.class);
    FileTime lastModifiedTime = FileTime.fromMillis(DateTime.now().getMillis());
    when(fs.supportsPathsWithScheme()).thenReturn(true);
    when(fs.getFileAttributes(any())).thenReturn(fileAttributes);
    when(fileAttributes.lastModifiedTime()).thenReturn(lastModifiedTime);
    when(fileAttributes.size()).thenReturn(1L);
    DremioInputFile inputFile = (DremioInputFile)io.newInputFile("dummy");
    long version = inputFile.getVersion();
    assertEquals("file version should be equal to the last file modification time", lastModifiedTime.toMillis(), version);
  }
}

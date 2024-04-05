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
import static org.junit.Assert.assertTrue;

import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/** Tests for {@link DeltaLakeFormatPlugin} */
public class TestDeltaLakeFormatPlugin {

  @Test
  public void testIsParquetCorrectExtension() throws Exception {
    final FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    final Path file = developTestParquet("zero_row.parquet");
    assertTrue(DeltaLakeFormatPlugin.isParquet(fs, fs.getFileAttributes(file)));
  }

  @Test
  public void testIsParquetNoExtension() throws Exception {
    final FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    final Path file = developTestParquet("000000_0");
    assertTrue(DeltaLakeFormatPlugin.isParquet(fs, fs.getFileAttributes(file)));
  }

  @Test
  public void testIsParquetNegative() throws Exception {
    final FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    final Path file = developTestParquet("000000_1");
    assertFalse(DeltaLakeFormatPlugin.isParquet(fs, fs.getFileAttributes(file)));
  }

  @Test
  public void testIsParquetInvalidSize() throws Exception {
    final FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    final Path file = developTestParquet("empty.parquet");
    assertFalse(DeltaLakeFormatPlugin.isParquet(fs, fs.getFileAttributes(file)));
  }

  private Path developTestParquet(String resourceName) throws IOException {
    File dest = Files.createTempFile(this.getClass().getName() + "_", ".parquet").toFile();
    Files.copy(
        Resources.getResource("parquet/" + resourceName).openStream(),
        dest.toPath(),
        StandardCopyOption.REPLACE_EXISTING);
    return Path.of(dest.getAbsolutePath());
  }
}

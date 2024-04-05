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
package com.dremio.exec.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.io.file.FileSystem;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/** Tests for {@code HadoopFileSystem} */
public class TestHadoopFileSystem {

  // Sanity check to make sure that HadoopFileSystem#getLocal(Configuration) actually returns an
  // instance of LocalFileSystem
  @Test
  public void testLocalFileSystem() throws IOException {
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    org.apache.hadoop.fs.LocalFileSystem localFS =
        fs.unwrap(org.apache.hadoop.fs.LocalFileSystem.class);
    assertThat(localFS).isNotNull();
  }

  // Sanity check to make sure that HadoopFileSystem#getLocal(Configuration) actually returns an
  // instance of RawLocalFileSystem
  @Test
  public void tesRawLocalFileSystem() throws IOException {
    FileSystem fs = HadoopFileSystem.getRawLocal(new Configuration());
    org.apache.hadoop.fs.RawLocalFileSystem localFS =
        fs.unwrap(org.apache.hadoop.fs.RawLocalFileSystem.class);
    assertThat(localFS).isNotNull();
  }
}

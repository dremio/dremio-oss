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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.dremio.common.util.FileUtils;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

/**
 * Tests for {@link DeltaLastCheckPointReader}
 */
public class TestDeltaLastCheckPointReader {

  @Test
  public void testVersionRead() throws IOException {
    File f = FileUtils.getResourceAsFile("/deltalake/_delta_log/_last_checkpoint");
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    Path path = Path.of(f.getAbsolutePath());
    Optional<Long> lastCheckPoint = DeltaLastCheckPointReader.getLastCheckPoint(fs, path);

    assertEquals(lastCheckPoint.get(), (Object)10L);
  }
}

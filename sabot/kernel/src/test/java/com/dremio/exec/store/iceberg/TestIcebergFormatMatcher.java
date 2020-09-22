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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

public class TestIcebergFormatMatcher {
  @ClassRule
  public static final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void match() throws Exception {
    IcebergFormatMatcher matcher = new IcebergFormatMatcher(null);
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    File root = tempDir.newFolder();
    FileSelection fileSelection = FileSelection.create(fs, Path.of(root.toURI()));
    boolean matched;

    // test with empty folder
    assertFalse(matcher.matches(fs, fileSelection, null));

    // empty "metadata" folder
    File metadata = new File(root, "metadata");
    metadata.mkdir();
    assertFalse(matcher.matches(fs, fileSelection, null));

    // add "version-hint.text"
    File versionHint = new File(metadata, "version-hint.text");
    versionHint.createNewFile();

    // missing dot after '9'
    File metadataJsonNoDot = new File(metadata, "v9metadata.json");
    metadataJsonNoDot.createNewFile();
    assertFalse(matcher.matches(fs, fileSelection, null));

    // all correct
    File metadataJson = new File(metadata, "v9.metadata.json");
    metadataJson.createNewFile();
    matched = matcher.matches(fs, fileSelection, null);
    assertTrue(matched);
  }
}

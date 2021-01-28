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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;

public class TestDeltaLakeFormatMatcher {

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void matches() throws Exception {
    DeltaLakeFormatPlugin plugin = mock(DeltaLakeFormatPlugin.class, RETURNS_DEEP_STUBS);
    when(plugin.getContext().getOptionManager().getOption(ExecConstants.ENABLE_DELTALAKE)).thenReturn(true);

    DeltaLakeFormatMatcher matcher = new DeltaLakeFormatMatcher(plugin);
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    File root = tempDir.newFolder();
    FileSelection fileSelection = FileSelection.create(fs, Path.of(root.toURI()));

    // test with empty folder
    assertFalse(matcher.matches(fs, fileSelection, null));

    // empty "_delta_log" folder
    File deltaLog = new File(root, "_delta_log");
    deltaLog.mkdir();

    // add "_last_checkpoint"
    File lastCheckpoint = new File(deltaLog, "_last_checkpoint");
    lastCheckpoint.createNewFile();

    //add
    File commitJson = new File(deltaLog, "00000000.json");
    commitJson.createNewFile();

    commitJson = new File(deltaLog, "0000001.json");
    commitJson.createNewFile();

    File checkpointParquet = new File(deltaLog, "000000000010.checkpoint.parquet");
    checkpointParquet.createNewFile();

    assertTrue(matcher.matches(fs, fileSelection, null));
  }

  @Test
  public void testAccessDenied() throws Exception {
    DeltaLakeFormatPlugin plugin = mock(DeltaLakeFormatPlugin.class, RETURNS_DEEP_STUBS);
    when(plugin.getContext().getOptionManager().getOption(ExecConstants.ENABLE_DELTALAKE)).thenReturn(true);

    DeltaLakeFormatMatcher matcher = new DeltaLakeFormatMatcher(plugin);
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    File root = tempDir.newFolder();
    FileSelection fileSelection = FileSelection.create(fs, Path.of(root.toURI()));

    // empty "_delta_log" folder
    File deltaLog = new File(root, "_delta_log");
    deltaLog.mkdir();

    // add "_last_checkpoint"
    File lastCheckpoint = new File(deltaLog, "_last_checkpoint");
    lastCheckpoint.createNewFile();

    //add commit file
    File commitJson = new File(deltaLog, "000001.json");
    commitJson.createNewFile();

    deltaLog.setReadable(false);

    assertFalse(matcher.matches(fs, fileSelection, null));

    //Setting readable again so is cleaned up after test
    deltaLog.setReadable(true);
  }

  @Test
  public void deltaLakeNotEnabled() throws Exception {
    DeltaLakeFormatPlugin plugin = mock(DeltaLakeFormatPlugin.class, RETURNS_DEEP_STUBS);
    when(plugin.getContext().getOptionManager().getOption(ExecConstants.ENABLE_DELTALAKE)).thenReturn(false);

    DeltaLakeFormatMatcher matcher = new DeltaLakeFormatMatcher(plugin);
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());
    File root = tempDir.newFolder();
    FileSelection fileSelection = FileSelection.create(fs, Path.of(root.toURI()));

    // empty "_delta_log" folder
    File deltaLog = new File(root, "_delta_log");
    deltaLog.mkdir();

    // add "_last_checkpoint"
    File lastCheckpoint = new File(deltaLog, "_last_checkpoint");
    lastCheckpoint.createNewFile();

    //add commit file
    File commitJson = new File(deltaLog, "000001.json");
    commitJson.createNewFile();

    assertFalse(matcher.matches(fs, fileSelection, null));
  }
}


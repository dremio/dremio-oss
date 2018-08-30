/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.dfs;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorStats;

public class TestFileSystemWrapper {

  private static String tempFilePath;

  @ClassRule
  public static final TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void createTempFile() throws Exception {

    File tempFile = tempFolder.newFile("dremioFSReadTest.txt");

    // Write some data
    PrintWriter printWriter = new PrintWriter(tempFile);
    for (int i=1; i<=200000; i++) {
      printWriter.println (String.format("%d, key_%d", i, i));
    }
    printWriter.close();

    tempFilePath = tempFile.getPath();
  }

  @Test
  public void testReadIOStats() throws Exception {
    FileSystemWrapper dfs = null;
    InputStream is = null;
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    OpProfileDef profileDef = new OpProfileDef(0 /*operatorId*/, 0 /*operatorType*/, 0 /*inputCount*/);
    OperatorStats stats = new OperatorStats(profileDef, null /*allocator*/);

    // start wait time method in OperatorStats expects the OperatorStats state to be in "processing"
    stats.startProcessing();

    try {
      dfs = new FileSystemWrapper(conf, stats, null);
      is = dfs.open(new Path(tempFilePath));

      byte[] buf = new byte[8000];
      while (is.read(buf, 0, buf.length) != -1) {
      }
    } finally {
      stats.stopProcessing();

      if (is != null) {
        is.close();
      }

      if (dfs != null) {
        dfs.close();
      }
    }

    OperatorProfile operatorProfile = stats.getProfile();
    assertTrue("Expected wait time is non-zero, but got zero wait time", operatorProfile.getWaitNanos() > 0);
  }

  @Test
  public void testWriteIOStats() throws Exception {
    FileSystemWrapper dfs = null;
    FSDataOutputStream os = null;
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    OpProfileDef profileDef = new OpProfileDef(0 /*operatorId*/, 0 /*operatorType*/, 0 /*inputCount*/);
    OperatorStats stats = new OperatorStats(profileDef, null /*allocator*/);

    // start wait time method in OperatorStats expects the OperatorStats state to be in "processing"
    stats.startProcessing();

    try {
      dfs = new FileSystemWrapper(conf, stats, null);
      os = dfs.create(new Path(tempFolder.getRoot().getPath(), "dremioFSWriteTest.txt"));

      byte[] buf = new byte[8192];
      for (int i = 0; i < 10000; ++i) {
        os.write(buf);
      }
    } finally {
      if (os != null) {
        os.close();
      }

      stats.stopProcessing();

      if (dfs != null) {
        dfs.close();
      }
    }

    OperatorProfile operatorProfile = stats.getProfile();
    assertTrue("Expected wait time is non-zero, but got zero wait time", operatorProfile.getWaitNanos() > 0);
  }
}

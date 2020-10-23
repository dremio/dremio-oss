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
package com.dremio.exec.store.hive.metadata;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.google.common.io.Resources;

/**
 * Basic tests for ParquetInputFormat.getSplits
 */
public class ParquetInputFormatTest extends TestCase {

  @Test
  public void testInvalidJobConf() throws Exception{

    final HiveConf hiveConf = new HiveConf();
    final JobConf jobConf = new JobConf(hiveConf);
    try {
      InputSplit[] inputSplits = new ParquetInputFormat().getSplits(jobConf, 1);
      fail();
    }
    catch (IOException ioe) {
      Assert.assertTrue(ioe.getMessage().contains("No input paths specified in job"));
    }
  }

  @Test
  public void testGetSplitsEmptyFolder() throws Exception{

    final HiveConf hiveConf = new HiveConf();
    final JobConf jobConf = new JobConf(hiveConf);
    TemporaryFolder temporaryFolder = new TemporaryFolder();
    temporaryFolder.create();
    File rootFolder = temporaryFolder.newFolder();
    jobConf.set("mapreduce.input.fileinputformat.inputdir", rootFolder.getAbsolutePath());
    InputSplit[] inputSplits = new ParquetInputFormat().getSplits(jobConf, 1);
    Assert.assertEquals(0, inputSplits.length);
  }

  @Test
  public void testGetSplitsNonEmptyFolder() throws Exception{

    final HiveConf hiveConf = new HiveConf();
    final JobConf jobConf = new JobConf(hiveConf);

    final File regionDir = new File(BaseTestQuery.getTempDir("region"));
    regionDir.mkdirs();
    final URL url = Resources.getResource("region.parquet");
    if (url == null) {
      throw new IOException(String.format("Unable to find path %s.", "region.parquet"));
    }

    final File file = new File(regionDir, "region.parquet");
    file.deleteOnExit();
    regionDir.deleteOnExit();
    Files.write(Paths.get(file.toURI()), Resources.toByteArray(url));

    jobConf.set("mapreduce.input.fileinputformat.inputdir", regionDir.getAbsolutePath());
    InputSplit[] inputSplits = new ParquetInputFormat().getSplits(jobConf, 1);
    Assert.assertEquals(1, inputSplits.length);
    for (InputSplit split : inputSplits) {
      Assert.assertTrue(split instanceof ParquetInputFormat.ParquetSplit);
      Assert.assertTrue(((ParquetInputFormat.ParquetSplit)split).getFileSize() > 0);
      Assert.assertTrue(((ParquetInputFormat.ParquetSplit)split).getModificationTime() > 0);
    }
  }

  @Test
  public void testGetSplitsMultiFileFolder() throws Exception{

    final HiveConf hiveConf = new HiveConf();
    final JobConf jobConf = new JobConf(hiveConf);

    final File regionDir = new File(BaseTestQuery.getTempDir("region"));
    regionDir.mkdirs();
    final URL url1 = Resources.getResource("region.parquet");
    final URL url2 = Resources.getResource("casetestdata.parquet");
    final URL url3 = Resources.getResource("complex_null_test.parquet");
    final URL url4 = Resources.getResource("deeply_nested_list.parquet");
    final URL url5 = Resources.getResource("multiple_rowgroups.parquet");
    final URL url6 = Resources.getResource("varchar_complex.parquet");
    final URL url7 = Resources.getResource("very_complex_float.parquet");

    if (url1 == null || url2 == null || url3 == null || url4 == null || url5 == null || url6 == null || url7 == null) {
      throw new IOException(String.format("Unable to find path %s.", "region.parquet"));
    }

    final File file1 = new File(regionDir, "region.parquet");
    final File file2 = new File(regionDir, "casetestdata.parquet");
    final File file3 = new File(regionDir, "complex_null_test.parquet");
    final File file4 = new File(regionDir, "deeply_nested_list.parquet");
    final File file5 = new File(regionDir, "multiple_rowgroups.parquet");
    final File file6 = new File(regionDir, "varchar_complex.parquet");
    final File file7 = new File(regionDir, "very_complex_float.parquet");

    file1.deleteOnExit();
    file2.deleteOnExit();
    file3.deleteOnExit();
    file4.deleteOnExit();
    file5.deleteOnExit();
    file6.deleteOnExit();
    file7.deleteOnExit();
    regionDir.deleteOnExit();
    Files.write(Paths.get(file1.toURI()), Resources.toByteArray(url1));
    Files.write(Paths.get(file2.toURI()), Resources.toByteArray(url2));
    Files.write(Paths.get(file3.toURI()), Resources.toByteArray(url3));
    Files.write(Paths.get(file4.toURI()), Resources.toByteArray(url4));
    Files.write(Paths.get(file5.toURI()), Resources.toByteArray(url5));
    Files.write(Paths.get(file6.toURI()), Resources.toByteArray(url6));
    Files.write(Paths.get(file7.toURI()), Resources.toByteArray(url7));

    jobConf.set("mapreduce.input.fileinputformat.inputdir", regionDir.getAbsolutePath());
    InputSplit[] inputSplits = new ParquetInputFormat().getSplits(jobConf, 1);
    Assert.assertEquals(7, inputSplits.length);
    for (InputSplit split : inputSplits) {
      Assert.assertTrue(split instanceof ParquetInputFormat.ParquetSplit);
      Assert.assertTrue(((ParquetInputFormat.ParquetSplit)split).getFileSize() > 0);
      Assert.assertTrue(((ParquetInputFormat.ParquetSplit)split).getModificationTime() > 0);
    }
  }
}

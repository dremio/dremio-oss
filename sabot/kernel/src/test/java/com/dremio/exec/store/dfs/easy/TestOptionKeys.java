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
package com.dremio.exec.store.dfs.easy;

import com.dremio.BaseTestQuery;
import com.dremio.exec.store.dfs.implicit.ImplicitFilesystemColumnFinder;
import com.dremio.options.OptionManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.junit.Assert;
import org.junit.Test;

public class TestOptionKeys extends BaseTestQuery {

  /*
  Verify that the file name is added to the query results when 'dremio.store.file.file-field-enabled' is enabled
  */
  @Test
  public void testFileFieldSettings() throws Exception {

    // DX-69983:
    //      dremio.store.file.file-field-enabled is on
    //      $file column will appear
    //      $file column values will be a relative (to the query) path + filename:  'enabled.json'
    //      prior to DX-69983 the full path would appear
    //
    // '/Users/userName/dremio/dremio/oss/sabot/kernel/target/randomNumber/file_field_enabled/enabled.json'

    Path root = Paths.get(getDfsTestTmpSchemaLocation(), "file_field_enabled");
    Files.createDirectories(root);
    Files.write(root.resolve("enabled.json"), "{a:1}".getBytes(), StandardOpenOption.CREATE);
    Files.write(root.resolve("labeled.json"), "{b:2}".getBytes(), StandardOpenOption.CREATE);
    Files.write(root.resolve("unlabeled.json"), "{c:3}".getBytes(), StandardOpenOption.CREATE);
    Files.write(root.resolve("disabled.json"), "{d:4}".getBytes(), StandardOpenOption.CREATE);

    // Test the "$file" column appears using dremio.store.file.file-field-enabled

    setSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_FILE_FIELD_ENABLE, true);

    testBuilder()
        .sqlQuery("select $file from dfs_test.file_field_enabled.\"enabled.json\"")
        .unOrdered()
        .baselineColumns("$file")
        .baselineValues("enabled.json")
        .build()
        .run();

    // Test the rename of the "$file" column using dremio.store.file.file-field-label

    setSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_FILE_FIELD_LABEL, "XX_FileName_XX");

    testBuilder()
        .sqlQuery("select XX_FileName_XX from dfs_test.file_field_enabled.\"labeled.json\"")
        .unOrdered()
        .baselineColumns("XX_FileName_XX")
        .baselineValues("labeled.json")
        .build()
        .run();

    // Reset to Off/Baseline and verify off
    resetSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_FILE_FIELD_LABEL);

    try {
      testBuilder()
          .sqlQuery("select XX_FileName_XX from dfs_test.file_field_enabled.\"unlabeled.json\"")
          .unOrdered()
          .baselineColumns("XX_FileName_XX")
          .baselineValues("unlabeled.json")
          .build()
          .run();
      Assert.fail("Failure: did not expect column 'XX_FileName_XX' to appear");
    } catch (Exception e) {
      // Failure Expected, the 'XX_FileName_XX' column should not appear in the table
      Assert.assertTrue(e.getMessage().contains("Column 'XX_FileName_XX' not found in any table"));
    }

    resetSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_FILE_FIELD_ENABLE);

    try {
      testBuilder()
          .sqlQuery("select $file from dfs_test.file_field_enabled.\"disabled.json\"")
          .unOrdered()
          .baselineColumns("$file")
          .baselineValues("disabled.json")
          .build()
          .run();
      Assert.fail("Failure: did not expect column '$file' to appear");
    } catch (Exception e) {
      // Failure Expected, the '$file' column should not appear in the table
      Assert.assertTrue(e.getMessage().contains("Column '$file' not found in any table"));
    }
  }

  /*
    Verify that the file name is added to the query results when 'dremio.store.file.file-field-enabled' is enabled
        and that the relative path/directory names appear

    The relative file paths is:
        "file_field_enabled/dirName1/dirName2/enabled2.json"
    the query is on:
        "file_field_enabled/dirName1"
    and so:
        "dirName2/enabled1.json"
    will appear in the "$file" column

    Structure:
       ./file_field_enabled/dirName1/enabled1.json
       ./file_field_enabled/dirName1/dirName2/enabled2.json
       ./file_field_enabled/dirName1/dirName2/dirName3/enabled3.json
  */
  @Test
  public void testFileFieldWithDirectorySettings() throws Exception {

    final OptionManager optionManager = getSabotContext().getOptionManager();

    Path root1 = Paths.get(getDfsTestTmpSchemaLocation(), "file_field_enabled", "dirName1");
    Files.createDirectories(root1);
    Files.write(root1.resolve("enabled1.json"), "{a:1}".getBytes(), StandardOpenOption.CREATE);

    Path root2 =
        Paths.get(getDfsTestTmpSchemaLocation(), "file_field_enabled", "dirName1", "dirName2");
    Files.createDirectories(root2);
    Files.write(root2.resolve("enabled2.json"), "{b:2}".getBytes(), StandardOpenOption.CREATE);

    Path root3 =
        Paths.get(
            getDfsTestTmpSchemaLocation(),
            "file_field_enabled",
            "dirName1",
            "dirName2",
            "dirName3");
    Files.createDirectories(root3);
    Files.write(root3.resolve("enabled3.json"), "{c:3}".getBytes(), StandardOpenOption.CREATE);

    // Test the "$file" column appears using dremio.store.file.file-field-enabled

    setSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_FILE_FIELD_ENABLE, true);

    testBuilder()
        .sqlQuery("select $file from dfs_test.file_field_enabled.dirName1")
        .unOrdered()
        .baselineColumns("$file")
        .baselineValues("enabled1.json")
        .baselineValues("dirName2/enabled2.json")
        .baselineValues("dirName2/dirName3/enabled3.json")
        .build()
        .run();
  }

  /*
  Verify that file modification time is added to the query results when 'dremio.store.file.mod-field-enabled' is enabled
  */
  @Test
  public void testModTimeSettings() throws Exception {

    Path root = Paths.get(getDfsTestTmpSchemaLocation(), "mod_field_enabled");
    Files.createDirectories(root);

    Files.write(root.resolve("enabled.json"), "{a:1}".getBytes(), StandardOpenOption.CREATE);
    long enabledMTime = Files.getLastModifiedTime(root.resolve("enabled.json")).toMillis();
    Files.write(root.resolve("disabled.json"), "{b:2}".getBytes(), StandardOpenOption.CREATE);
    long disabledMTime = Files.getLastModifiedTime(root.resolve("disabled.json")).toMillis();
    Files.write(root.resolve("labeled.json"), "{c:3}".getBytes(), StandardOpenOption.CREATE);
    long labeledMTime = Files.getLastModifiedTime(root.resolve("labeled.json")).toMillis();
    Files.write(root.resolve("unlabeled.json"), "{d:4}".getBytes(), StandardOpenOption.CREATE);
    long unlabeledMTime = Files.getLastModifiedTime(root.resolve("unlabeled.json")).toMillis();

    // Test the "$mtime" column appears using dremio.store.file.mod-field-enabled

    setSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_MOD_FIELD_ENABLE, true);

    testBuilder()
        .sqlQuery("select $mtime from dfs_test.mod_field_enabled.\"enabled.json\"")
        .unOrdered()
        .baselineColumns("$mtime")
        .baselineValues(enabledMTime)
        .build()
        .run();

    // Test the rename of the "$mtime" column using dremio.store.file.mod-field-label

    setSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_MOD_FIELD_LABEL, "XX_Modified_XX");

    testBuilder()
        .sqlQuery("select XX_Modified_XX from dfs_test.mod_field_enabled.\"labeled.json\"")
        .unOrdered()
        .baselineColumns("XX_Modified_XX")
        .baselineValues(labeledMTime)
        .build()
        .run();

    // Reset to Off/Baseline and verify off

    resetSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_MOD_FIELD_LABEL);

    try {
      testBuilder()
          .sqlQuery("select XX_Modified_XX from dfs_test.mod_field_enabled.\"unlabeled.json\"")
          .unOrdered()
          .baselineColumns("XX_Modified_XX")
          .baselineValues(unlabeledMTime)
          .build()
          .run();
      Assert.fail("Failure: did not expect column 'XX_Modified_XX' to appear");
    } catch (Exception e) {
      // Failure Expected, the 'XX_Modified_XX' column should not appear in the table
      Assert.assertTrue(e.getMessage().contains("Column 'XX_Modified_XX' not found in any table"));
    }

    resetSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_MOD_FIELD_ENABLE);

    try {
      testBuilder()
          .sqlQuery("select $mtime from dfs_test.mod_field_enabled.\"disabled.json\"")
          .unOrdered()
          .baselineColumns("$mtime")
          .baselineValues(disabledMTime)
          .build()
          .run();
      Assert.fail("Failure: did not expect column '$mtime' to appear");
    } catch (Exception e) {
      // Failure Expected, the '$mtime' column should not appear in the table
      Assert.assertTrue(e.getMessage().contains("Column '$mtime' not found in any table"));
    }
  }

  /*
  Verify that directory information is added to the query results when 'dremio.store.file.mod-field-enabled' is enabled

      Structure:
       ./dir_field_enabled/dirName1A/dirName2A/enabled1.json             (with "JsonPropName1")
       ./dir_field_enabled/dirName1A/dirName2A/dirName3A/enabled2.json   (with "JsonPropName1")
       ./dir_field_enabled/dirName1B/dirName2B/disabled1.json            (with "JsonPropName2")
       ./dir_field_enabled/dirName1B/dirName2B/dirName3B/disabled1.json  (with "JsonPropName2")
  */
  @Test
  public void testDirFieldSettings() throws Exception {

    Path root1 = Paths.get(getDfsTestTmpSchemaLocation(), "dirName1A", "dirName2A");
    Files.createDirectories(root1);
    Files.write(
        root1.resolve("enabled1.json"),
        "{JsonPropName1:\"abc\"}".getBytes(),
        StandardOpenOption.CREATE);

    Path root2 = Paths.get(getDfsTestTmpSchemaLocation(), "dirName1A", "dirName2A", "dirName3A");
    Files.createDirectories(root2);
    Files.write(
        root2.resolve("enabled2.json"),
        "{JsonPropName1:\"xyz\"}".getBytes(),
        StandardOpenOption.CREATE);

    Path root3 = Paths.get(getDfsTestTmpSchemaLocation(), "dirName1B", "dirName2B");
    Files.createDirectories(root3);
    Files.write(
        root3.resolve("disabled1.json"),
        "{JsonPropName2:\"abc\"}".getBytes(),
        StandardOpenOption.CREATE);

    Path root4 = Paths.get(getDfsTestTmpSchemaLocation(), "dirName1B", "dirName2B", "dirName3B");
    Files.createDirectories(root4);
    Files.write(
        root4.resolve("disabled2.json"),
        "{JsonPropName2:\"xyz\"}".getBytes(),
        StandardOpenOption.CREATE);

    // Test the directories appear as columns using dremio.store.file.dir-field-enabled

    setSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_DIRS_FIELD_ENABLE, true);

    testBuilder()
        .sqlQuery("select * from dfs_test.dirName1A")
        .unOrdered()
        .baselineColumns("dir0", "dir1", "JsonPropName1")
        .baselineValues("dirName2A", null, "abc")
        .baselineValues("dirName2A", "dirName3A", "xyz")
        .build()
        .run();

    testBuilder()
        .sqlQuery("select * from dfs_test.dirName1A.dirName2A")
        .unOrdered()
        .baselineColumns("dir0", "JsonPropName1")
        .baselineValues(null, "abc")
        .baselineValues("dirName3A", "xyz")
        .build()
        .run();

    // Set to Off/Baseline and verify off

    setSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_DIRS_FIELD_ENABLE, false);

    testBuilder()
        .sqlQuery("select * from dfs_test.dirName1B")
        .unOrdered()
        .baselineColumns("JsonPropName2")
        .baselineValues("abc")
        .baselineValues("xyz")
        .build()
        .run();

    resetSystemOption(ImplicitFilesystemColumnFinder.IMPLICIT_DIRS_FIELD_ENABLE);
  }
}

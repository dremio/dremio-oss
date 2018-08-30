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
package com.dremio.exec.physical.impl.xsort;

import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionMatcher;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

public class TestExternalSort extends BaseTestQuery {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private void attemptTestNumericTypes(String query, int record_count) throws Exception {
    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set \"exec.enable_union_type\" = true")
      .ordered()
      .baselineColumns("a");
    for (int i = record_count; i >= 0;) {
      builder.baselineValues((long) i--);
      if (i >= 0) {
        builder.baselineValues((double) i--);
      }
    }
    builder.go();
  }

  @Test
  public void testNumericTypes() throws Exception {
    final File table_dir = tempFolder.newFolder("numericTypes");
    final int record_count = 10000;
    BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "a.json")));
    String format = "{ a : %d }%n";
    for (int i = 0; i <= record_count; i += 2) {
      os.write(String.format(format, i).getBytes());
    }
    os.close();
    os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "b.json")));
    format = "{ a : %.2f }%n";
    for (int i = 1; i <= record_count; i+=2) {
      os.write(String.format(format, (float) i).getBytes());
    }
    os.close();
    String query = String.format("select * from dfs_root.\"%s\" order by a desc", table_dir.toPath().toString());
    // First attempt will fail with a schema change error
    try {
      attemptTestNumericTypes(query, record_count);
    } catch (Exception e) {
      UserExceptionMatcher m = new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.DATA_READ, "SCHEMA_CHANGE ERROR");
      @SuppressWarnings("deprecation") // deprecated methods used below: usage matches usage in AttemptManager
      final UserException expectedException = UserException.systemError(e).build();
      assertTrue(m.matches(expectedException));
    }
    // Second attempt should work, as we'd have learned the schema
    attemptTestNumericTypes(query, record_count);
  }

  private void attemptTestNumericAndStringTypes(String query, int record_count) throws Exception {
    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .ordered()
      .optionSettingQueriesForTestQuery("alter session set \"exec.enable_union_type\" = true")
      .baselineColumns("a");
    // Strings come first because order by is desc
    for (int i = record_count; i >= 0;) {
      i--;
      if (i >= 0) {
        builder.baselineValues(String.format("%05d", i--));
      }
    }
    for (int i = record_count; i >= 0;) {
      builder.baselineValues((long) i--);
      i--;
    }
    builder.go();
  }

  @Test
  public void testNumericAndStringTypes() throws Exception {
    final File table_dir = tempFolder.newFolder("numericAndStringTypes");
    final int record_count = 10000;
    BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "a.json")));
    String format = "{ a : %d }%n";
    for (int i = 0; i <= record_count; i += 2) {
      os.write(String.format(format, i).getBytes());
    }
    os.close();
    os = new BufferedOutputStream(new FileOutputStream(new File(table_dir, "b.json")));
    format = "{ a : \"%05d\" }%n";
    for (int i = 1; i <= record_count; i+=2) {
      os.write(String.format(format, i).getBytes());
    }
    os.close();
    String query = String.format("select * from dfs_root.\"%s\" order by a desc", table_dir.toPath().toString());
    // First attempt will fail with a schema change error
    try {
      attemptTestNumericAndStringTypes(query, record_count);
    } catch (Exception e) {
      UserExceptionMatcher m = new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.DATA_READ, "SCHEMA_CHANGE ERROR");
      @SuppressWarnings("deprecation") // deprecated methods used below: usage matches usage in AttemptManager
      final UserException expectedException = UserException.systemError(e).build();
      assertTrue(m.matches(expectedException));
    }
    // Second attempt should work, as we'd have learned the schema
    attemptTestNumericAndStringTypes(query, record_count);
  }
}

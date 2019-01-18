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
package com.dremio.exec.physical.impl.TopN;

import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionMatcher;

public class TestTopNSchemaChanges extends BaseTestQuery {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private void attemptTestNumericTypes(String query) throws Exception {
    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set \"exec.enable_union_type\" = true")
      .ordered()
      .baselineColumns("kl", "vl");

    for (long i = 0; i < 12; ++i) {
      if (i % 2 == 0) {
        builder.baselineValues(i, i);
      } else {
        builder.baselineValues((double) i, (double) i);
      }
    }
    builder.go();
  }

  @Test
  public void testNumericTypes() throws Exception {
    final File data_dir = tempFolder.newFolder("topn-schemachanges");

    // left side int and strings
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(data_dir, "d1.json")));
    for (int i = 0; i < 10000; i+=2) {
      writer.write(String.format("{ \"kl\" : %d , \"vl\": %d }\n", i, i));
    }
    writer.close();
    writer = new BufferedWriter(new FileWriter(new File(data_dir, "d2.json")));
    for (int i = 1; i < 10000; i+=2) {
      writer.write(String.format("{ \"kl\" : %f , \"vl\": %f }\n", (float)i, (float)i));
    }
    writer.close();
    String query = String.format("select * from dfs_root.\"%s\" order by kl limit 12", data_dir.toPath().toString());

    // First query will get a schema change error
    try {
      attemptTestNumericTypes(query);
    } catch (Exception e) {
      UserExceptionMatcher m = new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.DATA_READ, "SCHEMA_CHANGE ERROR");
      @SuppressWarnings("deprecation") // deprecated methods used below: usage matches usage in AttemptManager
        final UserException expectedException = UserException.systemError(e).build();
      assertTrue(m.matches(expectedException));
    }
    // Second attempt should work, as we'd have learned the schema
    attemptTestNumericTypes(query);
  }

  private void attemptTestNumericAndStringTypes(String query) throws Exception {
    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set \"exec.enable_union_type\" = true")
      .ordered()
      .baselineColumns("kl", "vl")
      .baselineValues("999", "999")
      .baselineValues("997", "997")
      .baselineValues("995", "995")
      .baselineValues("993", "993")
      .baselineValues("991", "991")
      .baselineValues("99", "99")
      .baselineValues("989", "989")
      .baselineValues("987", "987")
      .baselineValues("985", "985")
      .baselineValues("983", "983")
      .baselineValues("981", "981")
      .baselineValues("979", "979");
    builder.go();
  }

  @Test
  public void testNumericAndStringTypes() throws Exception {
    final File data_dir = tempFolder.newFolder("topn-schemachanges");

    // left side int and strings
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(data_dir, "d1.json")));
    for (int i = 0; i < 1000; i+=2) {
      writer.write(String.format("{ \"kl\" : %d , \"vl\": %d }\n", i, i));
    }
    writer.close();
    writer = new BufferedWriter(new FileWriter(new File(data_dir, "d2.json")));
    for (int i = 1; i < 1000; i+=2) {
      writer.write(String.format("{ \"kl\" : \"%s\" , \"vl\": \"%s\" }\n", i, i));
    }
    writer.close();
    String query = String.format("select * from dfs_root.\"%s\" order by kl desc limit 12", data_dir.toPath().toString());
    // First query will get a schema change error
    try {
      attemptTestNumericAndStringTypes(query);
    } catch (Exception e) {
      UserExceptionMatcher m = new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.DATA_READ, "SCHEMA_CHANGE ERROR");
      @SuppressWarnings("deprecation") // deprecated methods used below: usage matches usage in AttemptManager
      final UserException expectedException = UserException.systemError(e).build();
      assertTrue(m.matches(expectedException));
    }
    // Second attempt should work, as we'd have learned the schema
    attemptTestNumericAndStringTypes(query);
  }

  @Test
  public void testUnionTypes() throws Exception {
    final File data_dir = tempFolder.newFolder("topn-schemachanges");

    // union of int and float and string.
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(data_dir, "d1.json")));
    for (int i = 0; i <= 9; ++i) {
      switch (i%3) {
        case 0: // 0, 3, 6, 9
          writer.write(String.format("{ \"kl\" : %d , \"vl\": %d }\n", i, i));
          break;
        case 1: // 1, 4, 7
          writer.write(String.format("{ \"kl\" : %f , \"vl\": %f }\n", (float)i, (float)i));
          break;
        case 2: // 2, 5, 8
          writer.write(String.format("{ \"kl\" : \"%s\" , \"vl\": \"%s\" }\n", i, i));
          break;
      }
    }
    writer.close();
    String query = String.format("select * from dfs_root.\"%s\" order by kl limit 8", data_dir.toPath().toString());

    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set \"exec.enable_union_type\" = true")
      .ordered()
      .baselineColumns("kl", "vl")
      .baselineValues(0l, 0l)
      .baselineValues(1.0d, 1.0d)
      .baselineValues(3l, 3l)
      .baselineValues(4.0d, 4.0d)
      .baselineValues(6l, 6l)
      .baselineValues(7.0d, 7.0d)
      .baselineValues(9l, 9l)
      .baselineValues("2", "2");
    builder.go();
  }

}

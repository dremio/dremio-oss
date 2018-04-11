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
package com.dremio.exec.store;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.junit.Test;

import com.dremio.BaseTestQuery;

public class TestMetadataRefresh extends BaseTestQuery {

  @Test
  public void testRefresh() throws Exception {
    Path root = Paths.get(getDfsTestTmpSchemaLocation(), "blue", "metadata_refresh");
    Files.createDirectories(root);

    Files.write(root.resolve("f1.json"), "{a:1}".getBytes(), StandardOpenOption.CREATE);

    // load the metadata
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Metadata for table 'dfs_test.blue.metadata_refresh' refreshed.")
      .build().run();

    testBuilder()
      .sqlQuery("select count(*) as a from dfs_test.blue.metadata_refresh")
      .unOrdered()
      .baselineColumns("a")
      .baselineValues(1L)
      .build().run();

    // pause to ensure mtime changes on directory
    Thread.sleep(1200);

    // write a second file.
    Files.write(root.resolve("f2.json"), "{a:1}".getBytes(), StandardOpenOption.CREATE);

    // no change expected, metadata cached.
    testBuilder()
      .sqlQuery("select count(*) as a from dfs_test.blue.metadata_refresh")
      .unOrdered()
      .baselineColumns("a")
      .baselineValues(1L)
      .build().run();

    // refresh metadata
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Metadata for table 'dfs_test.blue.metadata_refresh' refreshed.")
      .build().run();

    // expect two rows.
    testBuilder()
      .sqlQuery("select count(*) as a from dfs_test.blue.metadata_refresh")
      .unOrdered()
      .baselineColumns("a")
      .baselineValues(2L)
      .build().run();

    // refresh again, no change expected.
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'dfs_test.blue.metadata_refresh' read signature reviewed but source stated metadata is unchanged, no refresh occurred.")
      .build().run();

    // delete the directory

    Files.delete(root.resolve("f1.json"));
    Files.delete(root.resolve("f2.json"));
    Files.delete(root);

    // refresh again, expect deletion.
    testBuilder()
      .sqlQuery("ALTER TABLE dfs_test.blue.metadata_refresh REFRESH METADATA")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Table 'dfs_test.blue.metadata_refresh' no longer exists, metadata removed.")
      .build().run();

  }
}

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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtSpecifierQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTagQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useContextQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginTruncate extends ITDataplanePluginTestSetup {

  @Test
  public void testTruncateWithAtAndUse() throws Exception {
    String devBranch = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName = "myTruncateTable";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createBranchAtSpecifierQuery(devBranch, mainBranch));
    // Use the branch main
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    // we have 'myTable' at main and devBranch
    runSQL(
        String.format("truncate %s.%s at branch %s", DATAPLANE_PLUGIN_NAME, tableName, devBranch));
    // we truncate 'myTable' at branch devBranch regardless of using main branch,
    // so we expect nothing in devBranch, but still have 1,2,3 in the main branch
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, devBranch)))
        .isEqualTo(Collections.emptyList());
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testTruncateWithAt() throws Exception {
    String devBranch = "devBranch";
    String mainBranch = String.format("BRANCH %s", DEFAULT_BRANCH_NAME);
    String tableName = "myTruncateTable";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createBranchAtSpecifierQuery(devBranch, mainBranch));
    // we have 'myTable' at main and devBranch
    runSQL(
        String.format("truncate %s.%s at branch %s", DATAPLANE_PLUGIN_NAME, tableName, devBranch));
    // we truncate 'myTable' at branch devBranch, so we expect nothing in devBranch, but still have
    // 1,2,3 in the main branch
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, devBranch)))
        .isEqualTo(Collections.emptyList());
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.singletonList(Arrays.asList("1", "2", "3")));
  }

  @Test
  public void testTruncateWithAtTag() throws Exception {
    String tagName = "myTag";
    String tableName = "myTruncateTableWithAt";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(createTagQuery(tagName, DEFAULT_BRANCH_NAME));
    // expect error for TAG
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "truncate %s.%s at tag %s", DATAPLANE_PLUGIN_NAME, tableName, tagName)))
        .isInstanceOf(UserRemoteException.class);
  }

  @Test
  public void testTruncate() throws Exception {
    String tableName = "myTruncateTable";
    runSQL(String.format("CREATE table %s.%s as select 1,2,3", DATAPLANE_PLUGIN_NAME, tableName));
    runSQL(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)));
    // we have 'myTable' at main and devBranch
    runSQL(String.format("truncate %s", tableName));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME, tableName, DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.emptyList());
  }

  @Test
  public void testNestedTruncate() throws Exception {
    String tableName = "myTruncateTable";
    final List<String> tablePath = tablePathWithFolders(tableName);

    runSQL(
        String.format(
            "CREATE table %s.%s.%s.%s as select 1,2,3",
            DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1), tableName));
    // we have 'myTable' at main and devBranch
    runSQL(
        String.format(
            "truncate %s.%s.%s.%s",
            DATAPLANE_PLUGIN_NAME, tablePath.get(0), tablePath.get(1), tableName));
    assertThat(
            runSqlWithResults(
                String.format(
                    "select * from %s.%s.%s.%s at branch %s",
                    DATAPLANE_PLUGIN_NAME,
                    tablePath.get(0),
                    tablePath.get(1),
                    tableName,
                    DEFAULT_BRANCH_NAME)))
        .isEqualTo(Collections.emptyList());
  }
}

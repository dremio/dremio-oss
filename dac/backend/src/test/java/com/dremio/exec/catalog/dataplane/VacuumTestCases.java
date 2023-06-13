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

import static com.dremio.BaseTestQuery.test;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.vacuumTableQuery;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.createFolders;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.test.UserExceptionAssert;

/**
 *
 * To run these tests, run through container class ITDataplanePlugin
 * To run all tests run {@link ITDataplanePlugin.NestedVacuumTests}
 * To run single test, see instructions at the top of {@link ITDataplanePlugin}
 */
public class VacuumTestCases {
  private ITDataplanePlugin base;

  VacuumTestCases(ITDataplanePlugin base) {
    this.base = base;
  }

  @Test
  public void expireSnapshotsOlderThan() throws Exception {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);

    // Set context to main branch
    base.runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    base.runSQL(createEmptyTableQuery(tablePath));

    final long expireSnapshotTimestamp = System.currentTimeMillis();

    // Test ExpireSnapshots query
    UserExceptionAssert.assertThatThrownBy(() ->
        test(vacuumTableQuery(tablePath, expireSnapshotTimestamp)))
      .hasErrorType(ErrorType.UNSUPPORTED_OPERATION)
      .hasMessageContaining("VACUUM TABLE command is not supported for this source");

    // Cleanup
    base.runSQL(dropTableQuery(tablePath));
  }
}

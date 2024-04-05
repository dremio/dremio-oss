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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableAsQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQueryWithoutSpecifyingSource;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.catalog.model.VersionContext;
import com.dremio.dac.api.SQLResource;
import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.service.jobs.JobsVersionContext;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.users.UserService;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Integration tests for the /sql REST API endpoints. <br>
 * <br>
 * Tests are done using SELECT because it is read-only, other queries would be valid too. Validation
 * is done using row counts just to make things simple.
 */
@SuppressWarnings("resource") // Many AutoCloseable's used here where calling close() is unnecessary
public class ITSqlRestApi extends ITBaseTestVersioned {
  private static final String TABLE_NAME = generateUniqueTableName();
  private static final List<String> TABLE_PATH = tablePathWithFolders(TABLE_NAME);
  private static final int TABLE_ROWS = 2;
  // Specifically chosen to not be near TABLE_ROWS value since we add a row or two sometimes
  private static final int TABLE_IN_BRANCH_ROWS = 7;
  private static final String ALTERNATE_BRANCH_NAME = generateUniqueBranchName();

  @BeforeAll
  public static void setup() throws Exception {
    // Prevents "requires identity" NPE
    SampleDataPopulator.addDefaultFirstUser(l(UserService.class), newNamespaceService());

    // Create alternate branch while empty
    runQuery(createBranchAtBranchQuery(ALTERNATE_BRANCH_NAME, DEFAULT_BRANCH_NAME));

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, TABLE_PATH, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(TABLE_PATH, TABLE_ROWS));

    // We re-use the same name as table #1, but with different row count
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, TABLE_PATH, VersionContext.ofBranch(ALTERNATE_BRANCH_NAME), null);
    runQuery(
        new SqlQuery(
            createTableAsQuery(TABLE_PATH, TABLE_IN_BRANCH_ROWS),
            Collections.emptyList(),
            DEFAULT_USERNAME,
            null,
            null,
            ImmutableMap.of(
                DATAPLANE_PLUGIN_NAME,
                new JobsVersionContext(
                    JobsVersionContext.VersionContextType.BRANCH, ALTERNATE_BRANCH_NAME))));
  }

  @ParameterizedTest
  @EnumSource(EndpointVersion.class)
  public void selectFromFullyQualifiedNameAndNullContext(EndpointVersion endpointVersion) {
    // Act
    final JobDataFragment tableResults =
        runSqlApiExpectSuccess(
            endpointVersion, new CreateFromSQL(selectStarQuery(TABLE_PATH), null));

    // Assert
    assertThat(tableResults.getReturnedRowCount()).isEqualTo(TABLE_ROWS);
  }

  @ParameterizedTest
  @EnumSource(EndpointVersion.class)
  public void selectWithSourceAsContext(EndpointVersion endpointVersion) {
    // Act
    final JobDataFragment tableResults =
        runSqlApiExpectSuccess(
            endpointVersion,
            new CreateFromSQL(
                selectStarQueryWithoutSpecifyingSource(TABLE_PATH),
                Collections.singletonList(DATAPLANE_PLUGIN_NAME)));

    // Assert
    assertThat(tableResults.getReturnedRowCount()).isEqualTo(TABLE_ROWS);
  }

  @ParameterizedTest
  @EnumSource(EndpointVersion.class)
  public void selectWithSourceAndFoldersAsContext(EndpointVersion endpointVersion) {
    // Act
    final JobDataFragment tableResults =
        runSqlApiExpectSuccess(
            endpointVersion,
            new CreateFromSQL(
                selectStarQueryWithoutSpecifyingSource(Collections.singletonList(TABLE_NAME)),
                Arrays.asList(DATAPLANE_PLUGIN_NAME, TABLE_PATH.get(0), TABLE_PATH.get(1))));

    // Assert
    assertThat(tableResults.getReturnedRowCount()).isEqualTo(TABLE_ROWS);
  }

  @ParameterizedTest
  @EnumSource(EndpointVersion.class)
  public void selectWithOneReference(EndpointVersion endpointVersion) {
    // Act
    final JobDataFragment tableResults =
        runSqlApiExpectSuccess(
            endpointVersion,
            new CreateFromSQL(
                selectStarQuery(TABLE_PATH),
                null,
                oneBranchReference(DATAPLANE_PLUGIN_NAME, ALTERNATE_BRANCH_NAME)));

    // Assert
    assertThat(tableResults.getReturnedRowCount()).isEqualTo(TABLE_IN_BRANCH_ROWS);
  }

  @ParameterizedTest
  @EnumSource(EndpointVersion.class)
  public void selectWithReferenceOverriddenByAtSyntax(EndpointVersion endpointVersion) {
    // Arrange
    // Add a row in a third branch so that it's different from the other branches
    final int rowsInsertedIntoThirdBranch = 1;
    final String thirdBranchName = generateUniqueBranchName();
    runQuery(createBranchAtBranchQuery(thirdBranchName, DEFAULT_BRANCH_NAME));
    insertRowsIntoTableInBranch(TABLE_PATH, thirdBranchName, rowsInsertedIntoThirdBranch);

    // Act
    // Expect "AT BRANCH thirdBranchName" to override alternateBranchReference
    final JobDataFragment tableResults =
        runSqlApiExpectSuccess(
            endpointVersion,
            new CreateFromSQL(
                String.format("%s AT BRANCH %s", selectStarQuery(TABLE_PATH), thirdBranchName),
                null,
                oneBranchReference(DATAPLANE_PLUGIN_NAME, ALTERNATE_BRANCH_NAME)));

    // Assert
    assertThat(tableResults.getReturnedRowCount())
        .isEqualTo(TABLE_ROWS + rowsInsertedIntoThirdBranch);
  }

  @ParameterizedTest
  @EnumSource(EndpointVersion.class)
  public void selectWithMultipleReferences(EndpointVersion endpointVersion) {
    // Arrange
    Map<String, VersionContextReq> references =
        twoBranchReferences(
            DATAPLANE_PLUGIN_NAME,
            DEFAULT_BRANCH_NAME,
            DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST,
            ALTERNATE_BRANCH_NAME);

    // We already have another source that happens to point to the same Nessie.
    // We can use it to access the same tables, but in another branch.
    final List<String> tableInSource2Path =
        Stream.concat(Stream.of(DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST), TABLE_PATH.stream())
            .collect(Collectors.toList());
    final String sqlQuery =
        String.format(
            "%s UNION ALL %s",
            selectStarQuery(TABLE_PATH),
            selectStarQueryWithoutSpecifyingSource(tableInSource2Path));

    // Act
    final JobDataFragment tableResults =
        runSqlApiExpectSuccess(endpointVersion, new CreateFromSQL(sqlQuery, null, references));

    // Assert
    // Union all, expect combination of both!
    assertThat(tableResults.getReturnedRowCount()).isEqualTo(TABLE_ROWS + TABLE_IN_BRANCH_ROWS);
  }

  @ParameterizedTest
  @EnumSource(EndpointVersion.class)
  public void selectWithUnnecessaryReferenceAreIgnored(EndpointVersion endpointVersion) {
    // Act
    final JobDataFragment tableResults =
        runSqlApiExpectSuccess(
            endpointVersion,
            new CreateFromSQL(
                selectStarQuery(TABLE_PATH),
                null,
                oneBranchReference("pluginThatDoesNotExist", ALTERNATE_BRANCH_NAME)));

    // Assert
    assertThat(tableResults.getReturnedRowCount()).isEqualTo(TABLE_ROWS);
  }

  @ParameterizedTest
  @EnumSource(EndpointVersion.class)
  public void invalidCommandFails(EndpointVersion endpointVersion) {
    // Arrange
    final String sql = "SHOW TAB";

    // Act + Assert
    runSqlApiExpectFailure(endpointVersion, new CreateFromSQL(sql, null));
  }

  @ParameterizedTest
  @EnumSource(EndpointVersion.class)
  public void insertIntoBranches(EndpointVersion endpointVersion) {
    // Arrange
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final int initialRowsInTable = 5;
    final String otherBranchName = generateUniqueBranchName();
    final int rowsInsertedIntoDefaultBranch = 1;
    final int rowsInsertedIntoOtherBranch = 2;

    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createTableAsQuery(tablePath, initialRowsInTable));
    runQuery(createBranchAtBranchQuery(otherBranchName, DEFAULT_BRANCH_NAME));

    // Act
    insertRowsIntoTableInBranch(tablePath, DEFAULT_BRANCH_NAME, rowsInsertedIntoDefaultBranch);
    insertRowsIntoTableInBranch(tablePath, otherBranchName, rowsInsertedIntoOtherBranch);

    // Assert
    final JobDataFragment tableInDefaultBranchResults =
        runSqlApiExpectSuccess(
            endpointVersion, new CreateFromSQL(selectStarQuery(tablePath), null));
    assertThat(tableInDefaultBranchResults.getReturnedRowCount())
        .isEqualTo(initialRowsInTable + rowsInsertedIntoDefaultBranch);

    final JobDataFragment tableInAlternateBranchResults =
        runSqlApiExpectSuccess(
            endpointVersion,
            new CreateFromSQL(
                selectStarQuery(tablePath),
                null,
                oneBranchReference(DATAPLANE_PLUGIN_NAME, otherBranchName)));
    assertThat(tableInAlternateBranchResults.getReturnedRowCount())
        .isEqualTo(initialRowsInTable + rowsInsertedIntoOtherBranch);
  }

  private enum EndpointVersion {
    API_V2,
    API_V3,
  }

  private JobDataFragment runSqlApiExpectSuccess(
      EndpointVersion endpointVersion, CreateFromSQL createFromSQL) {
    return expectSuccess(sqlApiInvocation(endpointVersion, createFromSQL), JobDataFragment.class);
  }

  private void runSqlApiExpectFailure(
      EndpointVersion endpointVersion, CreateFromSQL createFromSQL) {
    expectStatus(Response.Status.BAD_REQUEST, sqlApiInvocation(endpointVersion, createFromSQL));
  }

  private Invocation sqlApiInvocation(
      EndpointVersion endpointVersion, CreateFromSQL createFromSQL) {
    switch (endpointVersion) {
      case API_V2:
        // The /apiv2 implementation returns (up to 500) rows directly
        return getBuilder(getApiForVersion(endpointVersion).path("sql"))
            .buildPost(Entity.json(createFromSQL));
      case API_V3:
        // The /api/v3 implementation only returns a Job ID, if we want results, we need to fetch
        // them
        final SQLResource.QueryDetails queryDetails =
            expectSuccess(
                getBuilder(
                        getApiForVersion(endpointVersion) // Always v3 inside this case
                            .path("sql"))
                    .buildPost(Entity.json(createFromSQL)),
                SQLResource.QueryDetails.class);

        // Assuming "dremio.coordinator.rest.run_query.async" is false (by default, it is) then the
        // server will wait until the job is completed before returning our job ID

        // While there is a v3 equivalent (under /results instead of /data), we use v2 here so
        // that we can have the same response type of JobDataFragment for both cases.
        return getBuilder(
                getApiForVersion(EndpointVersion.API_V2)
                    .path("job")
                    .path(queryDetails.getId())
                    .path("data")
                    .queryParam("limit", 500)
                    .queryParam("offset", 0))
            .buildGet();
      default:
        throw new IllegalStateException("Unexpected value: " + endpointVersion);
    }
  }

  private static WebTarget getApiForVersion(EndpointVersion endpointVersion) {
    switch (endpointVersion) {
      case API_V2:
        return getAPIv2();
      case API_V3:
        return getPublicAPI(3);
      default:
        throw new IllegalStateException("Unexpected value: " + endpointVersion);
    }
  }

  private static Map<String, VersionContextReq> oneBranchReference(
      String sourceName, String branchName) {
    return Collections.singletonMap(
        sourceName, new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName));
  }

  @SuppressWarnings("SameParameterValue") // Only used once, but worth it for the clarity
  private static Map<String, VersionContextReq> twoBranchReferences(
      String sourceName1, String branchName1, String sourceName2, String branchName2) {
    return new HashMap<String, VersionContextReq>() {
      {
        put(
            sourceName1,
            new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName1));
        put(
            sourceName2,
            new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName2));
      }
    };
  }

  private void insertRowsIntoTableInBranch(
      List<String> tablePath, String branchName, int numberOfRows) {
    final String sessionId = runQueryAndGetSessionId(useBranchQuery(branchName));
    List<String> valuesToInsert =
        IntStream.rangeClosed(1, numberOfRows)
            .mapToObj(rowNumber -> String.format("(%d, %d)", rowNumber, rowNumber))
            .collect(Collectors.toList());

    // e.g. INSERT INTO my.table.path VALUES (1, 1), (2, 2), (3, 3)
    runQuery(insertTableWithValuesQuery(tablePath, valuesToInsert), sessionId);
  }
}

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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFunctionName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeFalse;

import com.dremio.dac.api.Space;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

@ExtendWith(OlderNessieServersExtension.class)
@NessieServerProperty(name = "nessie.test.storage.kind", value = "PERSIST")
public class ITTestSysUserDefinedFunctions extends ITBaseTestVersioned {
  private static final String DEFAULT_SPACE_NAME = "udfSpace";
  private static final NamespaceKey NONE_PATH = new NamespaceKey(ImmutableList.of("__none"));
  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() throws Exception {
    assumeFalse(isMultinode());
    allocator = getRootAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
    createSpace(DEFAULT_SPACE_NAME);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    dropSpace(DEFAULT_SPACE_NAME);
    allocator.close();
  }

  @Test
  public void sysUdfsWithOneVersionedUdf() throws JobNotFoundException {
    setSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(), "true");
    final String functionName1 = generateUniqueFunctionName();
    List<String> functionKey1 = tablePathWithFolders(functionName1);
    runQuery(createUdfQuery(functionKey1));

    JobDataFragment jobDataFragment =
        runQueryAndGetResults(
            String.format(
                "SELECT * FROM sys.user_defined_functions where name like '%%%s%%'",
                DATAPLANE_PLUGIN_NAME),
            allocator);
    assertThat(jobDataFragment.getReturnedRowCount() == 1).isTrue();
    assertThat(jobDataFragment.extractString("name", 0).contains(functionName1)).isTrue();

    runQuery(dropUdfQuery(functionKey1));
    jobDataFragment.close();
  }

  @Test
  public void sysUdfsWithOnlyVersionedUdfs() throws JobNotFoundException {
    setSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(), "true");
    final String functionName1 = generateUniqueFunctionName();
    List<String> functionKey1 = tablePathWithFolders(functionName1);
    runQuery(createUdfQuery(functionKey1));
    final String functionName2 = generateUniqueFunctionName();
    List<String> functionKey2 = tablePathWithFolders(functionName2);
    runQuery(createUdfQuery(functionKey2));
    final String functionName3 = generateUniqueFunctionName();
    List<String> functionKey3 = tablePathWithFolders(functionName3);
    runQuery(createUdfQuery(functionKey3));

    JobDataFragment jobDataFragment =
        runQueryAndGetResults(
            String.format(
                "SELECT * FROM sys.user_defined_functions where name like '%%%s%%' order by createdAt",
                DATAPLANE_PLUGIN_NAME),
            allocator);
    assertThat(jobDataFragment.getReturnedRowCount() == 3).isTrue();
    assertThat(jobDataFragment.extractString("name", 0).contains(functionName1)).isTrue();
    assertThat(jobDataFragment.extractString("name", 1).contains(functionName2)).isTrue();
    assertThat(jobDataFragment.extractString("name", 2).contains(functionName3)).isTrue();
    runQuery(dropUdfQuery(functionKey1));
    runQuery(dropUdfQuery(functionKey2));
    runQuery(dropUdfQuery(functionKey3));
    jobDataFragment.close();
  }

  @Test
  public void sysUdfsWithCombinationUdfs() throws JobNotFoundException {
    setSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(), "true");
    final String versionedUdf1 = generateUniqueFunctionName();
    List<String> versionedUdfKey1 = tablePathWithFolders(versionedUdf1);
    runQuery(createUdfQuery(versionedUdfKey1));
    final String versionedUdf2 = generateUniqueFunctionName();
    List<String> versionedUdfKey2 = tablePathWithFolders(versionedUdf2);
    runQuery(createUdfQuery(versionedUdfKey2));

    final String nonVersionedUdf = generateUniqueFunctionName();
    runQuery(createNonVersionedUdfQuery(Collections.singletonList(nonVersionedUdf)));

    JobDataFragment jobDataFragment =
        runQueryAndGetResults(
            String.format(
                "SELECT * FROM sys.user_defined_functions where name like '%%%s%%' OR name like '%%%s%%' order by createdAt",
                DATAPLANE_PLUGIN_NAME, nonVersionedUdf),
            allocator);
    assertThat(jobDataFragment.getReturnedRowCount() == 3).isTrue();
    assertThat(jobDataFragment.extractString("name", 0).contains(versionedUdf1)).isTrue();
    assertThat(jobDataFragment.extractString("name", 1).contains(versionedUdf2)).isTrue();
    assertThat(jobDataFragment.extractString("name", 2).contains(nonVersionedUdf)).isTrue();
    runQuery(dropUdfQuery(versionedUdfKey1));
    runQuery(dropUdfQuery(versionedUdfKey2));
    runQuery(dropNonVersionedUdfQuery(Collections.singletonList(nonVersionedUdf)));
    jobDataFragment.close();
  }

  @Test
  public void sysUdfsWithNonVersionedUdfs() throws JobNotFoundException {
    setSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(), "true");

    final String nonVersionedUdf = generateUniqueFunctionName();
    runQuery(createNonVersionedUdfQuery(Collections.singletonList(nonVersionedUdf)));
    JobDataFragment jobDataFragment =
        runQueryAndGetResults(String.format("SELECT * FROM sys.user_defined_functions"), allocator);
    assertThat(jobDataFragment.getReturnedRowCount() == 1).isTrue();

    assertThat(jobDataFragment.extractString("name", 0).contains(nonVersionedUdf)).isTrue();
    runQuery(dropNonVersionedUdfQuery(Collections.singletonList(nonVersionedUdf)));
    jobDataFragment.close();
  }

  @Test
  public void sysUdfsWithSupportKeyOff() throws JobNotFoundException {
    setSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(), "true");
    final String functionName1 = generateUniqueFunctionName();
    List<String> functionKey1 = tablePathWithFolders(functionName1);
    runQuery(createUdfQuery(functionKey1));
    final String functionName2 = generateUniqueFunctionName();
    List<String> functionKey2 = tablePathWithFolders(functionName2);
    runQuery(createUdfQuery(functionKey2));
    final String functionName3 = generateUniqueFunctionName();
    List<String> functionKey3 = tablePathWithFolders(functionName3);
    runQuery(createUdfQuery(functionKey3));
    final String nonVersionedUdf = generateUniqueFunctionName();
    runQuery(createNonVersionedUdfQuery(Collections.singletonList(nonVersionedUdf)));
    setSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(), "false");
    JobDataFragment jobDataFragment =
        runQueryAndGetResults(
            String.format(
                "SELECT * FROM sys.user_defined_functions where name like '%%%s%%' OR name like '%%%s%%' order by createdAt",
                DATAPLANE_PLUGIN_NAME, nonVersionedUdf),
            allocator);
    assertThat(jobDataFragment.getReturnedRowCount() == 1).isTrue();
    assertThat(jobDataFragment.extractString("name", 0).contains(nonVersionedUdf)).isTrue();
    jobDataFragment.close();
    setSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED.getOptionName(), "true");
    runQuery(dropUdfQuery(functionKey1));
    runQuery(dropUdfQuery(functionKey2));
    runQuery(dropUdfQuery(functionKey3));
    dropNonVersionedUdfQuery(Collections.singletonList(nonVersionedUdf));
  }

  protected void createSpace(String name) {
    expectSuccess(
        getBuilder(getPublicAPI(3).path("/catalog/"))
            .buildPost(Entity.json(new com.dremio.dac.api.Space(null, name, null, null, null))),
        new GenericType<Space>() {});
  }

  protected void dropSpace(String name) {
    com.dremio.dac.api.Space s =
        expectSuccess(
            getBuilder(getPublicAPI(3).path("/catalog/").path("by-path").path(name)).buildGet(),
            new GenericType<com.dremio.dac.api.Space>() {});
    expectSuccess(getBuilder(getPublicAPI(3).path("/catalog/").path(s.getId())).buildDelete());
  }

  public static String createNonVersionedUdfQuery(final List<String> functionPath) {
    Preconditions.checkNotNull(functionPath);
    return String.format(
        "CREATE FUNCTION %s.%s (x INT, y INT) RETURNS INT RETURN SELECT x * y",
        DEFAULT_SPACE_NAME, joinedTableKey(functionPath));
  }

  public static String dropNonVersionedUdfQuery(final List<String> functionPath) {
    Preconditions.checkNotNull(functionPath);
    return String.format("DROP FUNCTION %s.%s ", DEFAULT_SPACE_NAME, joinedTableKey(functionPath));
  }
}

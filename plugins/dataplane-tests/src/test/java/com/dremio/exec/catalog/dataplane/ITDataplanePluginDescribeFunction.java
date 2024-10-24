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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.CREATE_FUNCTION;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.INT32_FUNCTION_TYPE;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeUdfQueryWithoutPluginName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateEntityPathWithNFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFolderName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFunctionName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueSpaceName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useContextQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useFolderQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveEntity;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasFunction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.service.namespace.NamespaceException;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.UDF;

public class ITDataplanePluginDescribeFunction extends ITDataplanePluginFunctionBase {

  @Test
  public void describeUdfInDefaultFails() throws Exception {
    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);
    assertThatThrownBy(() -> runSQL(describeUdfQuery(functionKey)))
        .hasMessageContaining("Cannot find function with name");
  }

  @Test
  public void describeUdfInDefaultExistsInDefault() throws Exception {
    // Set-Up
    final String functionName = generateUniqueFunctionName();
    final List<String> functionKey = tablePathWithFolders(functionName);

    // Act / Assert
    runSQL(createUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);
  }

  @Test
  public void describeUdfInOtherFails() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));

    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    assertNessieDoesNotHaveEntity(functionKey, otherBranch, this);
    assertThatThrownBy(() -> runSQL(describeUdfQueryWithAt(functionKey, otherBranch)))
        .hasMessageContaining("Cannot find function with name");
  }

  @Test
  public void describeInOtherExistsInDefaultFails() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(describeUdfQueryWithAt(functionKey, otherBranch)))
        .hasMessageContaining("Cannot find function with name");
  }

  @Test
  public void describeUdfInDefaultExistsInOtherFails() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(describeUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME)))
        .hasMessageContaining("Cannot find function with name");
  }

  @Test
  public void describeUdfInOtherExistsInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Act / Assert
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);
  }

  @Test
  public void describeUdfWithContext() throws Exception {
    // Set-Up
    final String functionName = generateUniqueFunctionName();
    final List<String> functionKey = generateEntityPathWithNFolders(functionName, 2);
    final String folder0 = functionKey.get(0);
    final String folder1 = functionKey.get(1);
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    runSQL(useFolderQuery(ImmutableList.of(DATAPLANE_PLUGIN_NAME, folder0, folder1)));

    // Act / Assert
    assertUdfContentWithoutPluginName(ImmutableList.of(functionName), CREATE_FUNCTION);
  }

  @Test
  public void describeUdfWithWrongContextFails() throws Exception {
    // Set-Up
    final String functionName = generateUniqueFunctionName();
    final List<String> functionKey = generateEntityPathWithNFolders(functionName, 2);
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThatThrownBy(
            () -> runSQL(describeUdfQueryWithoutPluginName(ImmutableList.of(functionName))))
        .hasMessageContaining("Cannot find function with name");
  }

  @Test
  public void describeUdfWithMetadataNotAccessible() throws Exception {
    // Set-Up
    final List<String> functionKey = ImmutableList.of(generateUniqueFunctionName());
    // Directly commit a UDF to Nessie with the metadataLocation not accessible
    getNessieApi()
        .commitMultipleOperations()
        .branch(getNessieApi().getDefaultBranch())
        .commitMeta(CommitMeta.fromMessage("test"))
        .operation(
            Operation.Put.of(
                ContentKey.of(functionKey),
                UDF.udf("random_path_not_accessible", "versionId", "signatureId")))
        .commit();

    // Act / Assert
    assertThatThrownBy(() -> runSQL(describeUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME)))
        .hasMessageContaining("Cannot find function with name");
  }

  private static Stream<Arguments> provideDescribeFunctionAtRoot() {
    return Stream.<Arguments>builder()
        .add(Arguments.of(ImmutableList.of(), generateUniqueFolderName(), "SELECT 1"))
        .add(
            () -> {
              final String folderName = generateUniqueFolderName();
              return Arguments.of(ImmutableList.of(DATAPLANE_PLUGIN_NAME), folderName, "SELECT 2")
                  .get();
            })
        .add(
            () -> {
              final String folderName = generateUniqueFolderName();
              return Arguments.of(
                      ImmutableList.of(DATAPLANE_PLUGIN_NAME, folderName), folderName, "SELECT 4")
                  .get();
            })
        .add(
            () -> {
              final String folderName = generateUniqueFolderName();
              final String space = generateUniqueSpaceName();
              try {
                createSpaceInNamespace(space);
              } catch (NamespaceException e) {
                throw new RuntimeException(e);
              }
              return Arguments.of(ImmutableList.of(space), folderName, "SELECT 1").get();
            })
        .build();
  }

  @ParameterizedTest
  @MethodSource("provideDescribeFunctionAtRoot")
  public void describeAtRoot(List<String> context, String folderName, String functionBody)
      throws Exception {
    // Set-Up
    final String functionName = generateUniqueFunctionName();
    createUdfs(folderName, functionName);
    runSQL(useContextQuery(context));

    // Act / Assert
    assertUdfContentWithSql(
        String.format("DESCRIBE FUNCTION %s", functionName), INT32_FUNCTION_TYPE, functionBody);
  }

  private static Stream<Arguments> provideDescribeFunctionAtRootAtBranch() {
    return Stream.<Arguments>builder()
        .add(
            Arguments.of(
                ImmutableList.of(),
                generateUniqueFolderName(),
                null,
                "Version context 'BRANCH main' not supported in"))
        .add(
            () -> {
              final String folderName = generateUniqueFolderName();
              return Arguments.of(
                      ImmutableList.of(DATAPLANE_PLUGIN_NAME), folderName, "SELECT 2", null)
                  .get();
            })
        .add(
            () -> {
              final String folderName = generateUniqueFolderName();
              return Arguments.of(
                      ImmutableList.of(DATAPLANE_PLUGIN_NAME, folderName),
                      folderName,
                      "SELECT 4",
                      null)
                  .get();
            })
        .add(
            () -> {
              final String folderName = generateUniqueFolderName();
              final String space = generateUniqueSpaceName();
              try {
                createSpaceInNamespace(space);
              } catch (NamespaceException e) {
                throw new RuntimeException(e);
              }
              return Arguments.of(
                      ImmutableList.of(space),
                      folderName,
                      null,
                      "Version context 'BRANCH main' not supported in")
                  .get();
            })
        .build();
  }

  @ParameterizedTest
  @MethodSource("provideDescribeFunctionAtRootAtBranch")
  public void describeAtRootAtBranch(
      List<String> context, String folderName, String functionBody, String errorMessage)
      throws Exception {
    // Set-Up
    final String functionName = generateUniqueFunctionName();
    createUdfs(folderName, functionName);
    runSQL(useContextQuery(context));

    // Act / Assert
    String describeFunctionSql =
        String.format("DESCRIBE FUNCTION %s AT BRANCH \"main\"", functionName);
    if (StringUtils.isBlank(errorMessage)) {
      assertUdfContentWithSql(describeFunctionSql, INT32_FUNCTION_TYPE, functionBody);
    } else {
      assertThatThrownBy(() -> runSQL(describeFunctionSql)).hasMessageContaining(errorMessage);
    }
  }

  private static Stream<Arguments> provideDescribeFunctionAtNessie() {
    return Stream.<Arguments>builder()
        .add(Arguments.of(ImmutableList.of(), generateUniqueFolderName()))
        .add(
            () -> {
              final String folderName = generateUniqueFolderName();
              return Arguments.of(ImmutableList.of(DATAPLANE_PLUGIN_NAME), folderName).get();
            })
        .add(
            () -> {
              final String folderName = generateUniqueFolderName();
              return Arguments.of(ImmutableList.of(DATAPLANE_PLUGIN_NAME, folderName), folderName)
                  .get();
            })
        .add(
            () -> {
              final String folderName = generateUniqueFolderName();
              final String space = generateUniqueSpaceName();
              try {
                createSpaceInNamespace(space);
              } catch (NamespaceException e) {
                throw new RuntimeException(e);
              }
              return Arguments.of(ImmutableList.of(space), folderName).get();
            })
        .build();
  }

  @ParameterizedTest
  @MethodSource("provideDescribeFunctionAtNessie")
  public void describeAtNessie(List<String> context, String folderName) throws Exception {
    // Set-Up
    final String functionName = generateUniqueFunctionName();
    createUdfs(folderName, functionName);
    runSQL(useContextQuery(context));

    // Act / Assert
    assertUdfContentWithSql(
        String.format("DESCRIBE FUNCTION %s.%s", DATAPLANE_PLUGIN_NAME, functionName),
        INT32_FUNCTION_TYPE,
        "SELECT 2");
  }

  @ParameterizedTest
  @MethodSource("provideDescribeFunctionAtNessie")
  public void describeAtNessieAtBranch(List<String> context, String folderName) throws Exception {
    // Set-Up
    final String functionName = generateUniqueFunctionName();
    createUdfs(folderName, functionName);
    runSQL(useContextQuery(context));

    // Act / Assert
    assertUdfContentWithSql(
        String.format(
            "DESCRIBE FUNCTION %s.%s AT BRANCH \"main\"", DATAPLANE_PLUGIN_NAME, functionName),
        INT32_FUNCTION_TYPE,
        "SELECT 2");
  }

  private void createUdfs(String folderName, String functionName) throws Exception {
    // 1. Create a udf at the root of all things
    runSQL(String.format("CREATE FUNCTION %s() RETURNS INT RETURN 1", functionName));
    // 2. Create a udf in Nessie root
    runSQL(
        String.format(
            "CREATE FUNCTION %s.%s () RETURNS INT RETURN 2", DATAPLANE_PLUGIN_NAME, functionName));
    // 3. Create a udf in a folder in Nessie
    runSQL(
        String.format(
            "CREATE FUNCTION %s.%s.%s () RETURNS INT RETURN 4",
            DATAPLANE_PLUGIN_NAME, folderName, functionName));
  }
}

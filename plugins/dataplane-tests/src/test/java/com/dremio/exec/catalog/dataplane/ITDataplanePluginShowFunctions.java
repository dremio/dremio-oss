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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFunctionName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueSpaceName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.showAllUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginShowFunctions extends ITDataplanePluginFunctionBase {

  @Test
  public void showUdfsWithUdfInDefault() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    final String functionName = functionKey.get(functionKey.size() - 1);
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));

    // Act / Assert
    assertThat(
            runSqlWithResults(showAllUdfQuery()).stream()
                .filter(f -> f.get(0).contains(functionName)))
        .isNotEmpty();
  }

  @Test
  public void showUdfsWithUdfInOther() throws Exception {
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    final String functionName = functionKey.get(functionKey.size() - 1);
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));

    // Act / Assert
    assertThat(
            runSqlWithResults(showAllUdfQuery()).stream()
                .filter(f -> f.get(0).contains(functionName)))
        .isEmpty();
  }

  @Test
  public void showUdfsWithUdfInOtherUseOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    final String functionName = functionKey.get(functionKey.size() - 1);
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    runSQL(useBranchQuery(otherBranch));

    // Act / Assert
    assertThat(
            runSqlWithResults(showAllUdfQuery()).stream()
                .filter(f -> f.get(0).contains(functionName)))
        .isEmpty();
  }

  @Test
  public void showUdfsWithUdfInNamespaceAndNessie() throws Exception {
    // Set-Up
    // Create a udf in Namespace
    final String nameSpace = generateUniqueSpaceName();
    createSpaceInNamespace(nameSpace);
    final String namespaceFunctionName = generateUniqueFunctionName();
    runSQL(createUdfQuery(nameSpace, List.of(namespaceFunctionName)));

    // Create a udf in Nessie
    final List<String> nessieUdfFunctionKey = generateFunctionKeyWithFunctionInFolder();
    final String nessieUdfFunctionName = nessieUdfFunctionKey.get(nessieUdfFunctionKey.size() - 1);
    runSQL(createUdfQuery(nessieUdfFunctionKey));

    // Act / Assert
    final List<List<String>> functions = runSqlWithResults(showAllUdfQuery());
    assertThat(functions.stream().filter(f -> f.get(0).contains(namespaceFunctionName)))
        .isNotEmpty();
    assertThat(functions.stream().filter(f -> f.get(0).contains(nessieUdfFunctionName)))
        .isNotEmpty();
  }

  @Test
  public void showUdfsWithUdfInNamespaceAndNessieWhenFlagOff() throws Exception {
    // Set-Up
    // Create a udf in Namespace
    final String nameSpace = generateUniqueSpaceName();
    createSpaceInNamespace(nameSpace);
    final String namespaceFunctionName = generateUniqueFunctionName();
    runSQL(createUdfQuery(nameSpace, List.of(namespaceFunctionName)));

    // Create a udf in Nessie
    final List<String> nessieUdfFunctionKey = generateFunctionKeyWithFunctionInFolder();
    final String nessieUdfFunctionName = nessieUdfFunctionKey.get(nessieUdfFunctionKey.size() - 1);
    runSQL(createUdfQuery(nessieUdfFunctionKey));

    // disable versioned udf
    try (AutoCloseable ignored = disableVersionedSourceUdf()) {
      // Act / Assert
      final List<List<String>> functions = runSqlWithResults(showAllUdfQuery());
      assertThat(functions.stream().filter(f -> f.get(0).contains(namespaceFunctionName)))
          .isNotEmpty();
      assertThat(functions.stream().filter(f -> f.get(0).contains(nessieUdfFunctionName)))
          .isEmpty();
    }
  }

  @Test
  public void showUdfsWithUdfsInMultipleVersionedSources() throws Exception {
    // Set-Up
    final List<String> functionKeyOne = generateFunctionKeyWithFunctionInFolder();
    final String functionNameOne = functionKeyOne.get(functionKeyOne.size() - 1);
    runSQL(createUdfQuery(functionKeyOne));

    setupForAnotherPlugin();
    final List<String> functionKeyTwo = generateFunctionKeyWithFunctionInFolder();
    final String functionNameTwo = functionKeyTwo.get(functionKeyTwo.size() - 1);
    runSQL(createUdfQuery(DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST, functionKeyTwo));

    // Act / Assert
    final List<List<String>> functions = runSqlWithResults(showAllUdfQuery());
    assertThat(functions.stream().filter(f -> f.get(0).contains(functionNameOne))).isNotEmpty();
    assertThat(functions.stream().filter(f -> f.get(0).contains(functionNameTwo))).isNotEmpty();
  }
}

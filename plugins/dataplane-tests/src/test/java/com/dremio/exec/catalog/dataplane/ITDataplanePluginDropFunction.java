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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchFromBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieDoesNotHaveEntity;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasFunction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginDropFunction extends ITDataplanePluginFunctionBase {

  @Test
  public void dropUdfInDefaultFails() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(dropUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME)))
        .hasMessageContaining("Function")
        .hasMessageContaining("does not exist");
  }

  @Test
  public void dropUdfInDefaultExistsInDefault() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    runSQL(dropUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropUdfInDefaultExistsInOtherFails() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(dropUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME)))
        .hasMessageContaining("Function")
        .hasMessageContaining("does not exist");
    assertNessieHasFunction(functionKey, otherBranch, this);
  }

  @Test
  public void dropUdfInOtherFails() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    assertNessieDoesNotHaveEntity(functionKey, otherBranch, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(dropUdfQueryWithAt(functionKey, otherBranch)))
        .hasMessageContaining("Function")
        .hasMessageContaining("does not exist");
  }

  @Test
  public void dropUdfInOtherExistsInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Act / Assert
    runSQL(dropUdfQueryWithAt(functionKey, otherBranch));
    assertNessieDoesNotHaveEntity(functionKey, otherBranch, this);
  }

  @Test
  public void dropUdfInOtherExistsInDefaultFails() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(dropUdfQueryWithAt(functionKey, otherBranch)))
        .hasMessageContaining("Function")
        .hasMessageContaining("does not exist");
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropUdfAsTable() throws Exception {
    // Set-Up
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(dropTableQuery(functionKey)))
        .hasMessageContaining("does not exist");
  }

  @Test
  public void dropUdfAsView() throws Exception {
    // Set-Up
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(dropViewQuery(functionKey)))
        .hasMessageContaining("VALIDATION ERROR: Unknown view");
  }

  @Test
  public void dropViewAsUdf() throws Exception {
    // Set-Up
    List<String> tableKey = tablePathWithFolders(generateUniqueTableName());
    runSQL(createEmptyTableQuery(tableKey));
    List<String> viewKey = tablePathWithFolders(generateUniqueViewName());
    runSQL(createViewQuery(viewKey, tableKey));

    // Act / Assert
    assertThatThrownBy(() -> runSQL(dropUdfQuery(viewKey))).hasMessageContaining("does not exist");
  }

  @Test
  public void dropUdfWithSessionVersionContext() throws Exception {
    // Set-Up
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(otherBranch));
    runSQL(createUdfQuery(functionKey));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);

    // Act / Assert
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    assertThatThrownBy(() -> runSQL(dropUdfQuery(functionKey)))
        .hasMessageContaining("does not exist");

    runSQL(useBranchQuery(otherBranch));
    runSQL(dropUdfQuery(functionKey));
    assertNessieDoesNotHaveEntity(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void dropUdfWithContext() throws Exception {
    // Set-Up
    String otherBranch = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);

    // Act / Assert
    assertThatThrownBy(() -> runSQL(dropUdfQuery(functionKey)))
        .hasMessageContaining("does not exist");

    runSQL(useBranchQuery(otherBranch));
    runSQL(dropUdfQuery(functionKey));
    assertNessieDoesNotHaveEntity(functionKey, otherBranch, this);
  }
}

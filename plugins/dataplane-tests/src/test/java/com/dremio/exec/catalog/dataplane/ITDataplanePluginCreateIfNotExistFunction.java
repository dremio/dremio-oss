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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.CREATE_FUNCTION_IF_NOT_EXISTS;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.INT32_FUNCTION_TYPE;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchFromBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfIfNotExistsQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasFunction;

import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginCreateIfNotExistFunction extends ITDataplanePluginFunctionBase {

  @Test
  public void createUdfIfNotExistsInDefault() throws Exception {
    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfIfNotExistsQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(
        functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION_IF_NOT_EXISTS);
  }

  @Test
  public void createUdfIfNotExistsInDefaultExistsInDefault() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    runSQL(createUdfIfNotExistsQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);
  }

  @Test
  public void createUdfIfNotExistsInDefaultExistsInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    runSQL(createUdfIfNotExistsQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(
        functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION_IF_NOT_EXISTS);
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);
  }

  @Test
  public void createUdfIfNotExistsInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));

    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfIfNotExistsQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION_IF_NOT_EXISTS);
  }

  @Test
  public void createUdfIfNotExistsInOtherExistsInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    runSQL(createUdfIfNotExistsQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);
  }

  @Test
  public void createUdfIfNotExistsInOtherExistsInDefault() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    runSQL(createUdfIfNotExistsQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION_IF_NOT_EXISTS);
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);
  }
}

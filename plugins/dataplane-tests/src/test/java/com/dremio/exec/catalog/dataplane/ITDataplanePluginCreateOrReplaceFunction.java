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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.ARRAY_FUNCTION_TYPE;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.COMPLEX_FUNCTION;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.CREATE_FUNCTION;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.CREATE_OR_REPLACE_FUNCTION;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.INT32_FUNCTION_TYPE;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchFromBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createOrReplaceComplexUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createOrReplaceUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasFunction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginCreateOrReplaceFunction extends ITDataplanePluginFunctionBase {

  @Test
  public void createOrReplaceUdfInDefault() throws Exception {
    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createOrReplaceUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(
        functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_OR_REPLACE_FUNCTION);
  }

  @Test
  public void createOrReplaceUdfInDefaultExistsInDefault() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    runSQL(createOrReplaceUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(
        functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_OR_REPLACE_FUNCTION);
  }

  @Test
  public void createOrReplaceUdfInDefaultExistsInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    runSQL(createOrReplaceUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(
        functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_OR_REPLACE_FUNCTION);
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);
  }

  @Test
  public void createOrReplaceUdfInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));

    // Act / Assert
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createOrReplaceUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_OR_REPLACE_FUNCTION);
  }

  @Test
  public void createOrReplaceUdfInOtherExistsInOther() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    runSQL(createOrReplaceUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_OR_REPLACE_FUNCTION);
  }

  @Test
  public void createOrReplaceUdfInOtherExistsInDefault() throws Exception {
    // Set-Up
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    runSQL(createOrReplaceUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_OR_REPLACE_FUNCTION);
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);
  }

  @Test
  public void createOrReplaceUdfWithSQLError() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQuery(functionKey));

    // Act / Assert
    assertThatThrownBy(
            () ->
                runSQL(
                    String.format(
                        "CREATE OR REPLACE FUNCTION %s.%s (x INT, y INT) RETURNS INT RETURN SELECT a FROM xyz",
                        DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey))))
        .hasMessageContaining("SYSTEM ERROR: SqlValidatorException");
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
  }

  @Test
  public void createOrReplaceComplexUdfInDefault() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    runSQL(createOrReplaceComplexUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, ARRAY_FUNCTION_TYPE, COMPLEX_FUNCTION);
  }
}

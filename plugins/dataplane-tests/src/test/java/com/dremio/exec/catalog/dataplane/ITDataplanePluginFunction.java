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
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.CREATE_OR_REPLACE_FUNCTION;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.INT32_FUNCTION_TYPE;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchFromBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createOrReplaceUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createUdfQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFunctionName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.mergeBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.TestDataplaneAssertions.assertNessieHasFunction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Lists;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ITDataplanePluginFunction extends ITDataplanePluginFunctionBase {

  @Test
  public void testCreateViewWithUnspecifiedUDFVersion() throws Exception {
    String table1 = generateUniqueTableName();
    List<String> table1Path = tablePathWithFolders(table1);
    runSQL(createEmptyTableQuery(table1Path));
    runSQL(insertTableQuery(table1Path));

    final String functionName = generateUniqueFunctionName();
    List<String> functionKey = tablePathWithFolders(functionName);
    String createBooleanUdf =
        String.format(
            "CREATE FUNCTION %s.%s () RETURNS BOOLEAN RETURN FALSE",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(functionKey));
    runSQL(createBooleanUdf);

    final String viewName = generateUniqueViewName();
    String createViewQuery =
        String.format(
            "CREATE VIEW %s.%s AS SELECT * FROM %s.%s AT BRANCH \"main\" WHERE %s.%s()",
            DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST,
            viewName,
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(table1Path),
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(functionKey));

    List<String> functionFullPath = Lists.newArrayList();
    functionFullPath.add(DATAPLANE_PLUGIN_NAME);
    functionFullPath.addAll(functionKey);
    String expectedContains =
        "VALIDATION ERROR: Version context for entity "
            + joinedTableKey(functionFullPath)
            + " must be specified using AT SQL syntax";
    assertThatThrownBy(() -> runSQL(createViewQuery)).hasMessageContaining(expectedContains);
  }

  @Test
  public void branchUdf() throws Exception {
    // Set-Up
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Act / Assert
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);
  }

  @Test
  public void mergeUdf() throws Exception {
    // Set-Up
    // Create a UDF in the default branch
    final List<String> functionKey = generateFunctionKeyWithFunctionInFolder();
    runSQL(createUdfQueryWithAt(functionKey, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Create a new branch
    final String otherBranch = generateUniqueBranchName();
    runSQL(createBranchFromBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_FUNCTION);

    // Replace the UDF in the new branch
    runSQL(createOrReplaceUdfQueryWithAt(functionKey, otherBranch));
    assertNessieHasFunction(functionKey, otherBranch, this);
    assertUdfContent(functionKey, otherBranch, INT32_FUNCTION_TYPE, CREATE_OR_REPLACE_FUNCTION);

    // Act / Assert
    runSQL(mergeBranchQuery(otherBranch, DEFAULT_BRANCH_NAME));
    assertNessieHasFunction(functionKey, DEFAULT_BRANCH_NAME, this);
    assertUdfContent(
        functionKey, DEFAULT_BRANCH_NAME, INT32_FUNCTION_TYPE, CREATE_OR_REPLACE_FUNCTION);
  }
}

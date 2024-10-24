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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.INT32_FUNCTION_TYPE;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.describeUdfQueryWithoutPluginName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateEntityPathWithNFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueFunctionName;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class ITDataplanePluginFunctionBase extends ITDataplanePluginTestSetup {

  @BeforeEach
  public void beforeEach() {
    enableVersionedSourceUdf();
  }

  @AfterEach
  public void afterEach() {
    //noinspection resource
    disableVersionedSourceUdf();
  }

  @NotNull
  protected static List<String> generateFunctionKeyWithFunctionInFolder() {
    return generateEntityPathWithNFolders(generateUniqueFunctionName(), 1);
  }

  protected void assertUdfContentWithoutPluginName(
      final List<String> functionKey, final String function) throws Exception {
    assertUdfContentWithSql(
        describeUdfQueryWithoutPluginName(functionKey), INT32_FUNCTION_TYPE, function);
  }

  protected void assertUdfResult(final String functionSql, final int functionResult)
      throws Exception {
    List<List<String>> results = runSqlWithResults(functionSql);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).get(0)).isEqualTo(String.valueOf(functionResult));
  }
}

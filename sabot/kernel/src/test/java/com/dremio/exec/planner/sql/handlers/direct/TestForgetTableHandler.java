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
package com.dremio.exec.planner.sql.handlers.direct;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.parser.SqlForgetTable;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;

@RunWith(MockitoJUnitRunner.class)
public class TestForgetTableHandler {
  private ForgetTableHandler forgetTableHandler;
  @Mock private Catalog catalog;

  private static final String TABLE_NAME = "my_table";
  private NamespaceKey TABLE_KEY;

  @Before
  public void setup() throws NamespaceException {
    forgetTableHandler = new ForgetTableHandler(catalog);

    when(catalog.resolveSingle(any(NamespaceKey.class))).thenAnswer((Answer<NamespaceKey>) invocationOnMock -> {
      NamespaceKey key = invocationOnMock.getArgument(0, NamespaceKey.class);
      if (key.equals(TABLE_KEY)) {
        return TABLE_KEY;
      }
      return null;
    });
  }

  @Test
  public void toResult_validations() throws Exception {
    List<String> rootPaths = Arrays.asList("@home", "sys", "INFORMATION_SCHEMA");

    for (String root: rootPaths) {
      TABLE_KEY = new NamespaceKey(Arrays.asList(root, TABLE_NAME));
      final SqlForgetTable forgetTable = new SqlForgetTable(
        SqlParserPos.ZERO,
        new SqlIdentifier(TABLE_KEY.getPathComponents(), SqlParserPos.ZERO));

      assertThatThrownBy(() -> forgetTableHandler.toResult("", forgetTable))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("FORGET METADATA is not supported on tables in homespace, sys, or INFORMATION_SCHEMA.");
    }
  }
}

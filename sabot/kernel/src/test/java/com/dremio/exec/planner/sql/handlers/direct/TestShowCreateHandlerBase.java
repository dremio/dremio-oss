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

import static com.dremio.exec.ExecConstants.SHOW_CREATE_ENABLED;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.mockito.Mock;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.ReferenceType;
import com.dremio.exec.planner.sql.parser.SqlShowCreate;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;

public abstract class TestShowCreateHandlerBase {
  protected static final String DEFAULT_BRANCH_NAME= "test";
  protected static final List<String> DEFAULT_VIEW_PATH = Arrays.asList("space", "view");
  protected static final NamespaceKey DEFAULT_VIEW_KEY = new NamespaceKey(DEFAULT_VIEW_PATH);
  protected static final List<String> DEFAULT_VERSIONED_VIEW_PATH = Arrays.asList("arctic", "versioned_view");
  protected static final NamespaceKey DEFAULT_VERSIONED_VIEW_KEY = new NamespaceKey(DEFAULT_VERSIONED_VIEW_PATH);

  protected static final List<String> DEFAULT_TABLE_PATH = Arrays.asList("space", "table");
  protected static final NamespaceKey DEFAULT_TABLE_KEY = new NamespaceKey(DEFAULT_TABLE_PATH);
  protected static final List<String> DEFAULT_VERSIONED_TABLE_PATH = Arrays.asList("arctic", "versioned_table");
  protected static final NamespaceKey DEFAULT_VERSIONED_TABLE_KEY = new NamespaceKey(DEFAULT_VERSIONED_TABLE_PATH);
  protected static final List<String> DEFAULT_TABLE_IN_SCRATCH_PATH = Arrays.asList("$scratch", "table");
  protected static final NamespaceKey DEFAULT_TABLE_IN_SCRATCH_KEY = new NamespaceKey(DEFAULT_TABLE_IN_SCRATCH_PATH);
  protected static SqlShowCreate SHOW_CREATE_VIEW = new SqlShowCreate(
    SqlParserPos.ZERO,
    true,
    new SqlIdentifier(DEFAULT_VIEW_PATH, SqlParserPos.ZERO),
    null,
    null
  );

  protected static SqlShowCreate SHOW_CREATE_VERSIONED_VIEW = new SqlShowCreate(
    SqlParserPos.ZERO,
    true,
    new SqlIdentifier(DEFAULT_VERSIONED_VIEW_PATH, SqlParserPos.ZERO),
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO)
  );

  protected static SqlShowCreate SHOW_CREATE_TABLE = new SqlShowCreate(
    SqlParserPos.ZERO,
    false,
    new SqlIdentifier(DEFAULT_TABLE_PATH, SqlParserPos.ZERO),
    null,
    null
  );

  protected static SqlShowCreate SHOW_CREATE_VERSIONED_TABLE = new SqlShowCreate(
    SqlParserPos.ZERO,
    false,
    new SqlIdentifier(DEFAULT_VERSIONED_TABLE_PATH, SqlParserPos.ZERO),
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO)
  );

  protected static SqlShowCreate SHOW_CREATE_TABLE_IN_SCRATCH = new SqlShowCreate(
    SqlParserPos.ZERO,
    false,
    new SqlIdentifier(DEFAULT_TABLE_IN_SCRATCH_PATH, SqlParserPos.ZERO),
    null,
    null
  );

  @Mock protected Catalog catalog;
  @Mock protected QueryContext context;
  @Mock protected OptionManager optionManager;
  @Mock protected UserSession userSession;
  @Mock protected VersionContext sessionVersion;
  @Mock protected ResolvedVersionContext resolvedVersionContext;
  @Mock protected DremioTable table;
  @Mock protected RelDataType type;
  @Mock protected RelDataTypeField field;

  @Before
  public void setup() {
    when(context.getOptions()).thenReturn(optionManager);
    when(optionManager.getOption(SHOW_CREATE_ENABLED)).thenReturn(true);
    when(context.getSession()).thenReturn(userSession);
  }
}

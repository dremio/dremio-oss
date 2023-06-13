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
package com.dremio.exec.planner.sql.handlers;

import static com.dremio.exec.ExecConstants.ENABLE_USE_VERSION_SYNTAX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.parser.ReferenceType;
import com.dremio.exec.planner.sql.parser.SqlAssignBranch;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.test.DremioTest;

/**
 * Tests for ALTER BRANCH ASSIGN.
 */
public class TestAssignBranchHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "localnessie";
  private static final String TARGET_BRANCH = "branch";
  private static final String DEFAULT_REFERENCE = "reference";
  private static final VersionContext DEFAULT_VERSION =
    VersionContext.ofBranch(TARGET_BRANCH);

  private OptionManager optionManager;
  private Catalog catalog;
  private AssignBranchHandler handler;
  private SqlAssignBranch assignBranch;
  private SqlAssignBranch assignBranchWithDefaultSource;
  private SqlAssignBranch assignBranchWithTag;
  private SqlAssignBranch assignBranchToItself;

  private DataplanePlugin dataplanePlugin;

  @Before
  public void setup() throws Exception {
    QueryContext context = mock(QueryContext.class);
    optionManager = mock(OptionManager.class);
    catalog = mock(Catalog.class);
    UserSession userSession = mock(UserSession.class);
    dataplanePlugin = mock(DataplanePlugin.class);

    when(context.getCatalog()).thenReturn(catalog);
    when(context.getOptions()).thenReturn(optionManager);
    when(context.getSession()).thenReturn(userSession);
    when(userSession.getDefaultSchemaPath()).thenReturn(new NamespaceKey(Arrays.asList(DEFAULT_SOURCE_NAME, "unusedFolder")));
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME)).thenReturn(DEFAULT_VERSION);
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalog.getSource(anyString())).thenReturn(dataplanePlugin);

    handler = new AssignBranchHandler(context);

    assignBranch =
        new SqlAssignBranch(
            SqlParserPos.ZERO,
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            ReferenceType.BRANCH,
            new SqlIdentifier(DEFAULT_REFERENCE, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    assignBranchWithDefaultSource =
        new SqlAssignBranch(
            SqlParserPos.ZERO,
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            ReferenceType.BRANCH,
            new SqlIdentifier(DEFAULT_REFERENCE, SqlParserPos.ZERO),
            null);

    assignBranchWithTag =
        new SqlAssignBranch(
            SqlParserPos.ZERO,
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            ReferenceType.TAG,
            new SqlIdentifier(DEFAULT_REFERENCE, SqlParserPos.ZERO),
            null);

    assignBranchToItself =
        new SqlAssignBranch(
            SqlParserPos.ZERO,
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            ReferenceType.BRANCH,
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

  }

  @Test
  public void assignBranchSucceed()
      throws ForemanSetupException, ReferenceConflictException, ReferenceNotFoundException {
    // Arrange
    doNothing().when(dataplanePlugin).assignBranch(anyString(), any());

    // Assert
    List<SimpleCommandResult> result = handler.toResult("", assignBranch);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);
    assertThat(result.get(0).summary)
      .contains("Assigned")
      .contains(TARGET_BRANCH)
      .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignBranchThrowUnsupport() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(false);

    // Assert
    assertThatThrownBy(() -> handler.toResult("", assignBranch))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("ALTER BRANCH")
      .hasMessageContaining("not supported");
  }

  @Test
  public void assignBranchThrowWrongPlugin() {
    // Arrange
    when(catalog.getSource(anyString())).thenReturn(null);

    // Assert
    assertThatThrownBy(() -> handler.toResult("", assignBranch))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support")
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignBranchThrowWrongSource() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(catalog.getSource(DEFAULT_SOURCE_NAME))
      .thenReturn(mock(StoragePlugin.class));

    assertThatThrownBy(() -> handler.toResult("", assignBranch))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support")
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }
  @Test
  public void assignBranchThrowNotFound()
      throws ReferenceConflictException, ReferenceNotFoundException {

    // Arrange
    doThrow(ReferenceNotFoundException.class)
        .when(dataplanePlugin)
        .assignBranch(anyString(), any());

    // Act+Assert
    assertThatThrownBy(() -> handler.toResult("", assignBranch))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("not found")
      .hasMessageContaining(DEFAULT_VERSION.toString())
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignBranchThrowConflict()
      throws ReferenceConflictException, ReferenceNotFoundException {
    // Arrange
    doThrow(ReferenceConflictException.class)
        .when(dataplanePlugin)
        .assignBranch(anyString(), any());

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", assignBranch))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("hash change")
      .hasMessageContaining(DEFAULT_VERSION.toString())
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignBranchWithDefaultSource() throws ReferenceNotFoundException, ReferenceConflictException, ForemanSetupException {
    // Arrange
    doNothing().when(dataplanePlugin).assignBranch(anyString(), any());

    // Act
    List<SimpleCommandResult> result = handler.toResult("", assignBranchWithDefaultSource);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("Assigned")
      .contains(DEFAULT_REFERENCE)
      .contains(DEFAULT_SOURCE_NAME);

  }
  @Test
  public void assignBranchWithTag() throws ReferenceNotFoundException, ReferenceConflictException, ForemanSetupException {
    // Arrange
    doNothing().when(dataplanePlugin).assignBranch(anyString(), any());

    // Act
    List<SimpleCommandResult> result = handler.toResult("", assignBranchWithTag);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("Assigned")
      .contains("tag")
      .contains(DEFAULT_REFERENCE)
      .contains(DEFAULT_SOURCE_NAME);

  }

  @Test
  public void assignBranchToItself () throws ReferenceNotFoundException, ReferenceConflictException, ForemanSetupException {
    // Arrange
    doNothing().when(dataplanePlugin).assignBranch(anyString(), any());

    // Act
    List<SimpleCommandResult> result = handler.toResult("", assignBranchToItself);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("Assigned")
      .contains("branch")
      .contains(TARGET_BRANCH)
      .contains(DEFAULT_SOURCE_NAME);
  }

}

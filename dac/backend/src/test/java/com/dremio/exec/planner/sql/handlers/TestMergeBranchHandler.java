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

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.parser.SqlMergeBranch;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.test.DremioTest;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;

/** Tests for ALTER BRANCH MERGE. */
public class TestMergeBranchHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "localnessie";
  private static final String TARGET_BRANCH = "targetbranch";
  private static final String SOURCE_BRANCH = "sourcebranch";
  private static final String DEFAULT_BRANCH_NAME = "branchName";
  private static final VersionContext DEFAULT_VERSION =
      VersionContext.ofBranch(DEFAULT_BRANCH_NAME);

  private QueryContext context;
  private OptionManager optionManager;
  private Catalog catalog;
  private UserSession userSession;
  private MergeBranchHandler handler;
  private DataplanePlugin dataplanePlugin;

  private MergeBranchHandler handlerVersionContext;
  private SqlMergeBranch mergeBranch;
  private SqlMergeBranch mergeBranchVersionContext;
  private SqlMergeBranch mergeBranchWithNoSource;
  private SqlMergeBranch mergeBranchWithNoTargetBranch;
  private SqlMergeBranch mergeBranchWithNoSourceAndTargetBranch;
  private DataplanePlugin dataplanePluginVersionContext;

  @Before
  public void setup() throws Exception {
    context = mock(QueryContext.class);
    optionManager = mock(OptionManager.class);
    catalog = mock(Catalog.class);
    userSession = mock(UserSession.class);
    dataplanePlugin = mock(DataplanePlugin.class);

    when(context.getCatalog()).thenReturn(catalog);
    when(context.getOptions()).thenReturn(optionManager);
    when(context.getSession()).thenReturn(userSession);
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalog.getSource(anyString())).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(userSession.getDefaultSchemaPath())
        .thenReturn(new NamespaceKey(Arrays.asList(DEFAULT_SOURCE_NAME, "unusedFolder")));
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME)).thenReturn(DEFAULT_VERSION);

    handler = new MergeBranchHandler(context);

    mergeBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    mergeBranchWithNoSource =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO),
            new SqlIdentifier(TARGET_BRANCH, SqlParserPos.ZERO),
            null);

    mergeBranchWithNoTargetBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    mergeBranchWithNoSourceAndTargetBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO, new SqlIdentifier(SOURCE_BRANCH, SqlParserPos.ZERO), null, null);
    setUpVersionContext();
  }

  public void setUpVersionContext() {
    QueryContext queryVersionContext = mock(QueryContext.class);
    OptionManager optionManagerVersionContext = mock(OptionManager.class);
    Catalog catalogVersionContext = mock(Catalog.class);
    UserSession userSessionVersionContext = mock(UserSession.class);
    dataplanePluginVersionContext = mock(DataplanePlugin.class);
    when(dataplanePluginVersionContext.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePluginVersionContext.unwrap(VersionedPlugin.class))
        .thenReturn(dataplanePluginVersionContext);
    when(queryVersionContext.getCatalog()).thenReturn(catalogVersionContext);
    when(queryVersionContext.getOptions()).thenReturn(optionManagerVersionContext);
    when(queryVersionContext.getSession()).thenReturn(userSessionVersionContext);
    when(optionManagerVersionContext.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalogVersionContext.getSource(anyString())).thenReturn(dataplanePluginVersionContext);
    when(dataplanePluginVersionContext.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(userSessionVersionContext.getSessionVersionForSource("localnessie"))
        .thenReturn(VersionContext.ofBranch("mainVersionContext"));
    handlerVersionContext = new MergeBranchHandler(queryVersionContext);
    mergeBranchVersionContext =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            new SqlIdentifier("sourcebranch", SqlParserPos.ZERO),
            null,
            new SqlIdentifier("localnessie", SqlParserPos.ZERO));
  }

  @Test
  public void mergeBranchSucceed()
      throws ForemanSetupException, ReferenceConflictException, ReferenceNotFoundException {
    doNothing().when(dataplanePlugin).mergeBranch(anyString(), anyString());

    List<SimpleCommandResult> result = handler.toResult("", mergeBranch);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);
  }

  @Test
  public void mergeBranchSucceedForVersionContext()
      throws ForemanSetupException,
          ReferenceAlreadyExistsException,
          ReferenceNotFoundException,
          NoDefaultBranchException,
          ReferenceConflictException {
    doNothing().when(dataplanePluginVersionContext).createTag(anyString(), any());
    List<SimpleCommandResult> result =
        handlerVersionContext.toResult("", mergeBranchVersionContext);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);
  }

  @Test
  public void mergeBranchThrowUnsupport() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(false);

    assertThatThrownBy(() -> handler.toResult("", mergeBranch)).isInstanceOf(UserException.class);
  }

  @Test
  public void createBranchThrowNotFoundForVersionContext()
      throws ReferenceNotFoundException, ReferenceConflictException {
    doThrow(ReferenceNotFoundException.class)
        .when(dataplanePluginVersionContext)
        .mergeBranch(anyString(), any());

    assertThatThrownBy(() -> handlerVersionContext.toResult("", mergeBranchVersionContext))
        .hasMessageContaining("mainVersionContext");
  }

  @Test
  public void mergeBranchThrowWrongPlugin() {
    when(catalog.getSource(anyString())).thenReturn(null);

    assertThatThrownBy(() -> handler.toResult("", mergeBranch)).isInstanceOf(UserException.class);
  }

  @Test
  public void mergeBranchThrowNotFound()
      throws ReferenceConflictException, ReferenceNotFoundException {
    doThrow(ReferenceNotFoundException.class)
        .when(dataplanePlugin)
        .mergeBranch(anyString(), anyString());

    assertThatThrownBy(() -> handler.toResult("", mergeBranch)).isInstanceOf(UserException.class);
  }

  @Test
  public void mergeBranchThrowConflict()
      throws ReferenceConflictException, ReferenceNotFoundException {
    doThrow(ReferenceConflictException.class)
        .when(dataplanePlugin)
        .mergeBranch(anyString(), anyString());

    assertThatThrownBy(() -> handler.toResult("", mergeBranch)).isInstanceOf(UserException.class);
  }

  @Test
  public void mergeBranchNoDefaultBranch()
      throws ReferenceConflictException, ReferenceNotFoundException {
    // TODO: Is this var needed?
    final SqlMergeBranch mergeBranch =
        new SqlMergeBranch(
            SqlParserPos.ZERO,
            new SqlIdentifier("sourcebranch", SqlParserPos.ZERO),
            null,
            new SqlIdentifier("localnessie", SqlParserPos.ZERO));

    doThrow(ReferenceNotFoundException.class)
        .when(dataplanePlugin)
        .mergeBranch(anyString(), anyString());

    assertThatThrownBy(() -> handler.toResult("", mergeBranch)).isInstanceOf(UserException.class);
  }

  @Test
  public void mergeBranchNoTargetBranch()
      throws ReferenceConflictException, ReferenceNotFoundException, ForemanSetupException {
    // Arrange
    doNothing().when(dataplanePlugin).mergeBranch(anyString(), anyString());
    // Act
    List<SimpleCommandResult> result = handler.toResult("", mergeBranchWithNoTargetBranch);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary).contains("merged").contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void mergeBranchEmptySourceUsesSessionContext()
      throws ReferenceNotFoundException, ReferenceConflictException, ForemanSetupException {
    // Arrange
    doNothing().when(dataplanePlugin).mergeBranch(anyString(), anyString());

    // Act
    List<SimpleCommandResult> result = handler.toResult("", mergeBranchWithNoSource);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary).contains("merged").contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void mergeBranchWithNoTargetBranchAndUseSessionContext()
      throws ReferenceNotFoundException, ReferenceConflictException, ForemanSetupException {
    // Arrange
    doNothing().when(dataplanePlugin).mergeBranch(anyString(), anyString());

    // Act
    List<SimpleCommandResult> result = handler.toResult("", mergeBranchWithNoSourceAndTargetBranch);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary).contains("merged").contains(DEFAULT_SOURCE_NAME);
  }
}

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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.parser.PrepositionType;
import com.dremio.exec.planner.sql.parser.ReferenceType;
import com.dremio.exec.planner.sql.parser.SqlCreateBranch;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.test.DremioTest;

/**
 * Tests for CREATE BRANCH SQL.
 */
public class TestCreateBranchHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "dataplane_source_1";
  private static final String NON_EXISTENT_SOURCE_NAME = "non_exist";
  private static final String SESSION_SOURCE_NAME = "session_source";
  private static final String DEFAULT_NEW_BRANCH_NAME = "new_branch";
  private static final String DEFAULT_BRANCH_NAME = "branchName";
  private static final VersionContext DEFAULT_VERSION =
    VersionContext.ofBranch(DEFAULT_BRANCH_NAME);
  private static final VersionContext SESSION_VERSION =
    VersionContext.ofBranch("session");
  private static final SqlCreateBranch DEFAULT_INPUT = new SqlCreateBranch(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_NEW_BRANCH_NAME, SqlParserPos.ZERO),
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    null,
    new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
    PrepositionType.AT);
  private static final SqlCreateBranch NO_SOURCE_INPUT = new SqlCreateBranch(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_NEW_BRANCH_NAME, SqlParserPos.ZERO),
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    null,
    null,
    PrepositionType.AT);
  private static final SqlCreateBranch NON_EXISTENT_SOURCE_INPUT = new SqlCreateBranch(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_NEW_BRANCH_NAME, SqlParserPos.ZERO),
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    null,
    new SqlIdentifier(NON_EXISTENT_SOURCE_NAME, SqlParserPos.ZERO),
    PrepositionType.AT);
  private static final SqlCreateBranch NO_VERSION_INPUT = new SqlCreateBranch(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_NEW_BRANCH_NAME, SqlParserPos.ZERO),
    null,
    null,
    null,
    new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
    PrepositionType.AT);
  private static final SqlCreateBranch IF_NOT_EXISTS_INPUT = new SqlCreateBranch(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_NEW_BRANCH_NAME, SqlParserPos.ZERO),
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    null,
    new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
    PrepositionType.AT);

  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private OptionManager optionManager;
  @Mock private Catalog catalog;
  @Mock private UserSession userSession;
  @Mock private DataplanePlugin dataplanePlugin;

  @InjectMocks private CreateBranchHandler handler;

  @Test
  public void createBranchSupportKeyDisabledThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(false);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("CREATE BRANCH")
      .hasMessageContaining("not supported");
  }

  @Test
  public void createBranchOnNonExistentSource() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    NamespaceNotFoundException notFoundException = new NamespaceNotFoundException("Cannot access");
    UserException nonExistException = UserException.validationError(notFoundException)
      .message("Tried to access non-existent source [%s].", NON_EXISTENT_SOURCE_NAME).buildSilently();
    when(userSession.getSessionVersionForSource(NON_EXISTENT_SOURCE_NAME)).thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getSource(NON_EXISTENT_SOURCE_NAME)).thenThrow(nonExistException);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NON_EXISTENT_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Tried to access non-existent source");
  }

  @Test
  public void createBranchEmptyReferenceUsesSessionVersion() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing()
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, SESSION_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", NO_VERSION_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("created")
      .contains(DEFAULT_NEW_BRANCH_NAME)
      .contains(SESSION_VERSION.toString())
      .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchEmptyReferenceUnspecifiedSessionUsesDefaultVersion() throws ForemanSetupException {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME))
      .thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getSource(DEFAULT_SOURCE_NAME))
      .thenReturn(dataplanePlugin);
    doNothing()
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, VersionContext.NOT_SPECIFIED);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", NO_VERSION_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("created")
      .contains(DEFAULT_NEW_BRANCH_NAME)
      .contains("the default branch")
      .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchEmptySourceUsesSessionContext() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPluginAndSessionContext();
    when(catalog.getSource(SESSION_SOURCE_NAME))
      .thenReturn(dataplanePlugin);
    doNothing()
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, DEFAULT_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", NO_SOURCE_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("created")
      .contains(DEFAULT_NEW_BRANCH_NAME)
      .contains(DEFAULT_VERSION.toString())
      .contains(SESSION_SOURCE_NAME);
  }

  @Test
  public void createBranchAtBranchSucceeds() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing()
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, DEFAULT_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", DEFAULT_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("created")
      .contains(DEFAULT_NEW_BRANCH_NAME)
      .contains(DEFAULT_VERSION.toString())
      .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchAtTagSucceeds() throws ForemanSetupException {
    // Constants
    final String tagName = "tagName";
    final SqlCreateBranch input = new SqlCreateBranch(
      SqlParserPos.ZERO,
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      new SqlIdentifier(DEFAULT_NEW_BRANCH_NAME, SqlParserPos.ZERO),
      ReferenceType.TAG,
      new SqlIdentifier(tagName, SqlParserPos.ZERO),
      null,
      new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
      PrepositionType.AT);

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing()
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, VersionContext.ofTag(tagName));

    // Act
    List<SimpleCommandResult> result = handler.toResult("", input);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("created")
      .contains(DEFAULT_NEW_BRANCH_NAME)
      .contains(tagName)
      .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchAtCommitSucceeds() throws ForemanSetupException {
    // Constants
    final String commitHash = "0123456789abcdeff";
    final SqlCreateBranch input = new SqlCreateBranch(
      SqlParserPos.ZERO,
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      new SqlIdentifier(DEFAULT_NEW_BRANCH_NAME, SqlParserPos.ZERO),
      ReferenceType.COMMIT,
      new SqlIdentifier(commitHash, SqlParserPos.ZERO),
      null,
      new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
      PrepositionType.AT);

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing()
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, VersionContext.ofCommit(commitHash));

    // Act
    List<SimpleCommandResult> result = handler.toResult("", input);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("created")
      .contains(DEFAULT_NEW_BRANCH_NAME)
      .contains(commitHash)
      .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchAtReferenceSucceeds() throws ForemanSetupException {
    // Constants
    final String referenceName = "refName";
    final SqlCreateBranch input = new SqlCreateBranch(
      SqlParserPos.ZERO,
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      new SqlIdentifier(DEFAULT_NEW_BRANCH_NAME, SqlParserPos.ZERO),
      ReferenceType.REFERENCE,
      new SqlIdentifier(referenceName, SqlParserPos.ZERO),
      null,
      new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
      PrepositionType.AT);

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing()
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, VersionContext.ofRef(referenceName));

    // Act
    List<SimpleCommandResult> result = handler.toResult("", input);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("created")
      .contains(DEFAULT_NEW_BRANCH_NAME)
      .contains(referenceName)
      .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchWrongSourceThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME))
      .thenReturn(SESSION_VERSION);
    when(catalog.getSource(DEFAULT_SOURCE_NAME))
      .thenReturn(mock(StoragePlugin.class));

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support")
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchWrongSourceFromContextThrows() {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPluginAndSessionContext();
    when(catalog.getSource(SESSION_SOURCE_NAME))
      .thenReturn(mock(StoragePlugin.class));

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NO_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support")
      .hasMessageContaining(SESSION_SOURCE_NAME);
  }

  @Test
  public void createBranchNullSourceFromContextThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(userSession.getDefaultSchemaPath())
      .thenReturn(null);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NO_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("was not specified");
  }

  @Test
  public void createBranchIfNotExistsDoesNotExistSucceeds() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing()
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, DEFAULT_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", IF_NOT_EXISTS_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("created")
      .contains(DEFAULT_NEW_BRANCH_NAME)
      .contains(DEFAULT_VERSION.toString())
      .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchIfNotExistsDoesExistNoOp() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceAlreadyExistsException.class)
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, DEFAULT_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", IF_NOT_EXISTS_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("already exists")
      .contains(DEFAULT_NEW_BRANCH_NAME)
      .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchAlreadyExistsThrows() {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceAlreadyExistsException.class)
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, DEFAULT_VERSION);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("already exists")
      .hasMessageContaining(DEFAULT_NEW_BRANCH_NAME)
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchNotFoundThrows() {
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceNotFoundException.class)
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, DEFAULT_VERSION);

    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("not found")
      .hasMessageContaining(DEFAULT_VERSION.toString())
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchNoDefaultBranchThrows() {
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(NoDefaultBranchException.class)
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, DEFAULT_VERSION);

    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not have a default branch")
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createBranchTypeConflictThrows() {
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceTypeConflictException.class)
      .when(dataplanePlugin)
      .createBranch(DEFAULT_NEW_BRANCH_NAME, DEFAULT_VERSION);

    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("is not the requested type")
      .hasMessageContaining(DEFAULT_VERSION.toString())
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  private void setUpSupportKeyAndSessionVersionAndPlugin() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME))
      .thenReturn(SESSION_VERSION);
    when(catalog.getSource(DEFAULT_SOURCE_NAME))
      .thenReturn(dataplanePlugin);
  }

  private void setUpSupportKeyAndSessionVersionAndPluginAndSessionContext() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(userSession.getSessionVersionForSource(SESSION_SOURCE_NAME))
      .thenReturn(SESSION_VERSION);
    when(catalog.getSource(SESSION_SOURCE_NAME))
      .thenReturn(dataplanePlugin);
    when(userSession.getDefaultSchemaPath())
      .thenReturn(new NamespaceKey(Arrays.asList(SESSION_SOURCE_NAME, "unusedFolder")));
  }

}

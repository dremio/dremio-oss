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

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.parser.ReferenceType;
import com.dremio.exec.planner.sql.parser.SqlCreateTag;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
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

/** Tests for CREATE TAG SQL. */
public class TestCreateTagHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "dataplane_source_1";
  private static final String NON_EXISTENT_SOURCE_NAME = "non_exist";
  private static final String SESSION_SOURCE_NAME = "session_source";
  private static final String DEFAULT_NEW_TAG_NAME = "new_tag";
  private static final String DEFAULT_BRANCH_NAME = "branchName";
  private static final VersionContext DEFAULT_VERSION =
      VersionContext.ofBranch(DEFAULT_BRANCH_NAME);
  private static final VersionContext SESSION_VERSION = VersionContext.ofBranch("session");
  private static final SqlCreateTag DEFAULT_INPUT =
      new SqlCreateTag(
          SqlParserPos.ZERO,
          SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
          new SqlIdentifier(DEFAULT_NEW_TAG_NAME, SqlParserPos.ZERO),
          ReferenceType.BRANCH,
          new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
          null,
          new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
          null);
  private static final SqlCreateTag NO_SOURCE_INPUT =
      new SqlCreateTag(
          SqlParserPos.ZERO,
          SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
          new SqlIdentifier(DEFAULT_NEW_TAG_NAME, SqlParserPos.ZERO),
          ReferenceType.BRANCH,
          new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
          null,
          null,
          null);
  private static final SqlCreateTag NON_EXISTENT_SOURCE_INPUT =
      new SqlCreateTag(
          SqlParserPos.ZERO,
          SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
          new SqlIdentifier(DEFAULT_NEW_TAG_NAME, SqlParserPos.ZERO),
          ReferenceType.BRANCH,
          new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
          null,
          new SqlIdentifier(NON_EXISTENT_SOURCE_NAME, SqlParserPos.ZERO),
          null);
  private static final SqlCreateTag NO_VERSION_INPUT =
      new SqlCreateTag(
          SqlParserPos.ZERO,
          SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
          new SqlIdentifier(DEFAULT_NEW_TAG_NAME, SqlParserPos.ZERO),
          null,
          null,
          null,
          new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
          null);
  private static final SqlCreateTag IF_NOT_EXISTS_INPUT =
      new SqlCreateTag(
          SqlParserPos.ZERO,
          SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
          new SqlIdentifier(DEFAULT_NEW_TAG_NAME, SqlParserPos.ZERO),
          ReferenceType.BRANCH,
          new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
          null,
          new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
          null);

  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private OptionManager optionManager;
  @Mock private Catalog catalog;
  @Mock private UserSession userSession;
  @Mock private DataplanePlugin dataplanePlugin;

  @InjectMocks private CreateTagHandler handler;

  @Test
  public void createTagSupportKeyDisabledThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(false);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("CREATE TAG")
        .hasMessageContaining("not supported");
  }

  @Test
  public void createTagNonExistentSource() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    NamespaceNotFoundException notFoundException = new NamespaceNotFoundException("Cannot access");
    UserException nonExistException =
        UserException.validationError(notFoundException)
            .message("Tried to access non-existent source [%s].", NON_EXISTENT_SOURCE_NAME)
            .buildSilently();
    when(userSession.getSessionVersionForSource(NON_EXISTENT_SOURCE_NAME))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getSource(NON_EXISTENT_SOURCE_NAME)).thenThrow(nonExistException);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NON_EXISTENT_SOURCE_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Tried to access non-existent source");
  }

  @Test
  public void createTagEmptyReferenceUsesSessionVersion() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing().when(dataplanePlugin).createTag(DEFAULT_NEW_TAG_NAME, SESSION_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", NO_VERSION_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("created")
        .contains(DEFAULT_NEW_TAG_NAME)
        .contains(SESSION_VERSION.toString())
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagEmptyReferenceUnspecifiedSessionUsesDefaultVersion()
      throws ForemanSetupException {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);
    doNothing().when(dataplanePlugin).createTag(DEFAULT_NEW_TAG_NAME, VersionContext.NOT_SPECIFIED);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", NO_VERSION_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("created")
        .contains(DEFAULT_NEW_TAG_NAME)
        .contains("the default branch")
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagEmptySourceUsesSessionContext() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPluginAndSessionContext();
    when(catalog.getSource(SESSION_SOURCE_NAME)).thenReturn(dataplanePlugin);
    doNothing().when(dataplanePlugin).createTag(DEFAULT_NEW_TAG_NAME, DEFAULT_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", NO_SOURCE_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("created")
        .contains(DEFAULT_NEW_TAG_NAME)
        .contains(DEFAULT_VERSION.toString())
        .contains(SESSION_SOURCE_NAME);
  }

  @Test
  public void createTagAtBranchSucceeds() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing().when(dataplanePlugin).createTag(DEFAULT_NEW_TAG_NAME, DEFAULT_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", DEFAULT_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("created")
        .contains(DEFAULT_NEW_TAG_NAME)
        .contains(DEFAULT_VERSION.toString())
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagAtTagSucceeds() throws ForemanSetupException {
    // Constants
    final String tagName = "tagName";
    final SqlCreateTag input =
        new SqlCreateTag(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_NEW_TAG_NAME, SqlParserPos.ZERO),
            ReferenceType.TAG,
            new SqlIdentifier(tagName, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            null);

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing()
        .when(dataplanePlugin)
        .createTag(DEFAULT_NEW_TAG_NAME, VersionContext.ofTag(tagName));

    // Act
    List<SimpleCommandResult> result = handler.toResult("", input);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("created")
        .contains(DEFAULT_NEW_TAG_NAME)
        .contains(tagName)
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagAtCommitSucceeds() throws ForemanSetupException {
    // Constants
    final String commitHash = "0123456789abcdeff";
    final SqlCreateTag input =
        new SqlCreateTag(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_NEW_TAG_NAME, SqlParserPos.ZERO),
            ReferenceType.COMMIT,
            new SqlIdentifier(commitHash, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            null);

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing()
        .when(dataplanePlugin)
        .createTag(DEFAULT_NEW_TAG_NAME, VersionContext.ofCommit(commitHash));

    // Act
    List<SimpleCommandResult> result = handler.toResult("", input);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("created")
        .contains(DEFAULT_NEW_TAG_NAME)
        .contains(commitHash)
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagAtReferenceSucceeds() throws ForemanSetupException {
    // Constants
    final String referenceName = "refName";
    final SqlCreateTag input =
        new SqlCreateTag(
            SqlParserPos.ZERO,
            SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
            new SqlIdentifier(DEFAULT_NEW_TAG_NAME, SqlParserPos.ZERO),
            ReferenceType.REFERENCE,
            new SqlIdentifier(referenceName, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO),
            null);

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing()
        .when(dataplanePlugin)
        .createTag(DEFAULT_NEW_TAG_NAME, VersionContext.ofRef(referenceName));

    // Act
    List<SimpleCommandResult> result = handler.toResult("", input);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("created")
        .contains(DEFAULT_NEW_TAG_NAME)
        .contains(referenceName)
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagWrongSourceThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME)).thenReturn(SESSION_VERSION);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(mock(StoragePlugin.class));

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("does not support")
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagWrongSourceFromContextThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(userSession.getSessionVersionForSource(SESSION_SOURCE_NAME)).thenReturn(SESSION_VERSION);
    when(catalog.getSource(SESSION_SOURCE_NAME)).thenReturn(dataplanePlugin);
    when(userSession.getDefaultSchemaPath())
        .thenReturn(new NamespaceKey(Arrays.asList(SESSION_SOURCE_NAME, "unusedFolder")));
    when(catalog.getSource(SESSION_SOURCE_NAME)).thenReturn(mock(StoragePlugin.class));

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NO_SOURCE_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("does not support")
        .hasMessageContaining(SESSION_SOURCE_NAME);
  }

  @Test
  public void createTagNullSourceFromContextThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(userSession.getDefaultSchemaPath()).thenReturn(null);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NO_SOURCE_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("was not specified");
  }

  @Test
  public void createTagIfNotExistsDoesNotExistSucceeds() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doNothing().when(dataplanePlugin).createTag(DEFAULT_NEW_TAG_NAME, DEFAULT_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", IF_NOT_EXISTS_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("created")
        .contains(DEFAULT_NEW_TAG_NAME)
        .contains(DEFAULT_VERSION.toString())
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagIfNotExistsDoesExistNoOp() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceAlreadyExistsException.class)
        .when(dataplanePlugin)
        .createTag(DEFAULT_NEW_TAG_NAME, DEFAULT_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", IF_NOT_EXISTS_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("already exists")
        .contains(DEFAULT_NEW_TAG_NAME)
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagAlreadyExistsThrows() {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceAlreadyExistsException.class)
        .when(dataplanePlugin)
        .createTag(DEFAULT_NEW_TAG_NAME, DEFAULT_VERSION);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("already exists")
        .hasMessageContaining(DEFAULT_NEW_TAG_NAME)
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagNotFoundThrows() {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceNotFoundException.class)
        .when(dataplanePlugin)
        .createTag(DEFAULT_NEW_TAG_NAME, DEFAULT_VERSION);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found")
        .hasMessageContaining(DEFAULT_VERSION.toString())
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagNoDefaultBranchThrows() {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(NoDefaultBranchException.class)
        .when(dataplanePlugin)
        .createTag(DEFAULT_NEW_TAG_NAME, DEFAULT_VERSION);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("does not have a default branch")
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void createTagTypeConflictThrows()
      throws ReferenceAlreadyExistsException,
          ReferenceNotFoundException,
          NoDefaultBranchException,
          ReferenceConflictException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceTypeConflictException.class)
        .when(dataplanePlugin)
        .createTag(DEFAULT_NEW_TAG_NAME, DEFAULT_VERSION);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("is not the requested type")
        .hasMessageContaining(DEFAULT_VERSION.toString())
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  private void setUpSupportKeyAndSessionVersionAndPlugin() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME)).thenReturn(SESSION_VERSION);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);
  }

  private void setUpSupportKeyAndSessionVersionAndPluginAndSessionContext() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(userSession.getSessionVersionForSource(SESSION_SOURCE_NAME)).thenReturn(SESSION_VERSION);
    when(catalog.getSource(SESSION_SOURCE_NAME)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);
    when(userSession.getDefaultSchemaPath())
        .thenReturn(new NamespaceKey(Arrays.asList(SESSION_SOURCE_NAME, "unusedFolder")));
  }
}

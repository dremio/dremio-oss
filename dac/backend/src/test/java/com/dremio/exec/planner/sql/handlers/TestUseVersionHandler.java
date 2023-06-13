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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.parser.ReferenceType;
import com.dremio.exec.planner.sql.parser.SqlUseVersion;
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
 * Tests for USE VERSION SQL.
 */
public class TestUseVersionHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "dataplane_source_1";
  private static final String NON_EXISTENT_SOURCE_NAME = "non_exist";
  private static final String SESSION_SOURCE_NAME = "session_source";
  private static final String DEFAULT_BRANCH_NAME = "branchName";
  private static final String DEFAULT_COMMIT_HASH = "0123456789abcdeff";
  private static final VersionContext DEFAULT_VERSION =
    VersionContext.ofBranch(DEFAULT_BRANCH_NAME);
  private static final VersionContext COMMIT_VERSION =
    VersionContext.ofBareCommit(DEFAULT_COMMIT_HASH);
  private static final SqlUseVersion DEFAULT_INPUT = new SqlUseVersion(
    SqlParserPos.ZERO,
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));
  private static final SqlUseVersion NO_SOURCE_INPUT = new SqlUseVersion(
    SqlParserPos.ZERO,
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    null);
  private static final SqlUseVersion NON_EXISTENT_SOURCE_INPUT = new SqlUseVersion(
    SqlParserPos.ZERO,
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    new SqlIdentifier(NON_EXISTENT_SOURCE_NAME, SqlParserPos.ZERO));
  private static final SqlUseVersion COMMIT_INPUT = new SqlUseVersion(
    SqlParserPos.ZERO,
    ReferenceType.COMMIT,
    new SqlIdentifier(DEFAULT_COMMIT_HASH, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));
  private static final ResolvedVersionContext DEFAULT_RESOLVED_VERSION =
    ResolvedVersionContext.ofBranch("expected", "ffedcba9876543210");
  private static final ResolvedVersionContext RESOLVED_COMMIT =
    ResolvedVersionContext.ofBareCommit(DEFAULT_COMMIT_HASH);

  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private OptionManager optionManager;
  @Mock private Catalog catalog;
  @Mock private UserSession userSession;
  @Mock private DataplanePlugin dataplanePlugin;

  @InjectMocks private UseVersionHandler handler;

  @Test
  public void useBranchSupportKeyDisabledThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(false);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("USE BRANCH")
      .hasMessageContaining("not supported");
    verify(userSession, never()).setSessionVersionForSource(any(), any());
  }

  @Test
  public void useBranchNonExistentSource() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    NamespaceNotFoundException notFoundException = new NamespaceNotFoundException("Cannot access");
    UserException nonExistException = UserException.validationError(notFoundException)
      .message("Tried to access non-existent source [%s].", NON_EXISTENT_SOURCE_NAME).build();
    when(catalog.getSource(NON_EXISTENT_SOURCE_NAME)).thenThrow(nonExistException);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NON_EXISTENT_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Tried to access non-existent source");
  }

  @Test
  public void useBranchEmptySourceUsesSessionContext() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndPluginAndSessionContext();
    when(dataplanePlugin.resolveVersionContext(DEFAULT_VERSION))
      .thenReturn(DEFAULT_RESOLVED_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", NO_SOURCE_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("set to")
      .contains(DEFAULT_VERSION.toString())
      .contains(SESSION_SOURCE_NAME);
    verify(userSession).setSessionVersionForSource(SESSION_SOURCE_NAME, DEFAULT_VERSION);
  }

  @Test
  public void useBranchSucceeds() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndPlugin();
    when(dataplanePlugin.resolveVersionContext(DEFAULT_VERSION))
      .thenReturn(DEFAULT_RESOLVED_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", DEFAULT_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("set to")
      .contains(DEFAULT_VERSION.toString())
      .contains(DEFAULT_SOURCE_NAME);
    verify(userSession).setSessionVersionForSource(DEFAULT_SOURCE_NAME, DEFAULT_VERSION);
  }

  @Test
  public void useTagSucceeds() throws ForemanSetupException {
    // Constants
    final String tagName = "tagName";
    final SqlUseVersion input = new SqlUseVersion(
      SqlParserPos.ZERO,
      ReferenceType.TAG,
      new SqlIdentifier(tagName, SqlParserPos.ZERO),
      new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));
    final VersionContext version = VersionContext.ofTag(tagName);

    // Arrange
    setUpSupportKeyAndPlugin();
    when(dataplanePlugin.resolveVersionContext(version))
      .thenReturn(ResolvedVersionContext.ofTag(tagName, DEFAULT_COMMIT_HASH));

    // Act
    List<SimpleCommandResult> result = handler.toResult("", input);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("set to")
      .contains(tagName)
      .contains(DEFAULT_SOURCE_NAME);
    verify(userSession).setSessionVersionForSource(DEFAULT_SOURCE_NAME, version);
  }

  @Test
  public void useCommitSucceeds() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndPlugin();
    when(dataplanePlugin.resolveVersionContext(COMMIT_VERSION))
      .thenReturn(RESOLVED_COMMIT);
    when(dataplanePlugin.commitExists(DEFAULT_COMMIT_HASH))
      .thenReturn(true);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", COMMIT_INPUT);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("set to")
      .contains(COMMIT_VERSION.toString())
      .contains(DEFAULT_SOURCE_NAME);
    verify(userSession).setSessionVersionForSource(DEFAULT_SOURCE_NAME, COMMIT_VERSION);
  }

  @Test
  public void useCommitNotFoundThrows() {
    // Arrange
    setUpSupportKeyAndPlugin();
    when(dataplanePlugin.resolveVersionContext(COMMIT_VERSION))
      .thenReturn(RESOLVED_COMMIT);
    when(dataplanePlugin.commitExists(DEFAULT_COMMIT_HASH))
      .thenReturn(false);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", COMMIT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Commit")
      .hasMessageContaining("not found")
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
    verify(userSession, never()).setSessionVersionForSource(any(), any());
  }

  @Test
  public void useReferenceSucceeds() throws ForemanSetupException {
    // Constants
    final String referenceName = "refName";
    final SqlUseVersion input = new SqlUseVersion(
      SqlParserPos.ZERO,
      ReferenceType.REFERENCE,
      new SqlIdentifier(referenceName, SqlParserPos.ZERO),
      new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));
    final VersionContext version = VersionContext.ofRef(referenceName);

    // Arrange
    setUpSupportKeyAndPlugin();
    when(dataplanePlugin.resolveVersionContext(version))
      .thenReturn(DEFAULT_RESOLVED_VERSION);

    // Act
    List<SimpleCommandResult> result = handler.toResult("", input);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("set to")
      .contains(referenceName)
      .contains(DEFAULT_SOURCE_NAME);
    verify(userSession).setSessionVersionForSource(DEFAULT_SOURCE_NAME, version);
  }

  @Test
  public void useBranchWrongSourceThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(catalog.getSource(DEFAULT_SOURCE_NAME))
      .thenReturn(mock(StoragePlugin.class));

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support")
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
    verify(userSession, never()).setSessionVersionForSource(any(), any());
  }

  @Test
  public void useBranchWrongSourceFromContextThrows() {
    // Arrange
    setUpSupportKeyAndPluginAndSessionContext();
    when(catalog.getSource(SESSION_SOURCE_NAME))
      .thenReturn(mock(StoragePlugin.class));

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NO_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support")
      .hasMessageContaining(SESSION_SOURCE_NAME);
    verify(userSession, never()).setSessionVersionForSource(any(), any());
  }

  @Test
  public void useBranchNullSourceFromContextThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(userSession.getDefaultSchemaPath())
      .thenReturn(null);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NO_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("was not specified");
    verify(userSession, never()).setSessionVersionForSource(any(), any());
  }

  @Test
  public void useBranchNotFoundThrows() {
    // Arrange
    setUpSupportKeyAndPlugin();
    doThrow(ReferenceNotFoundException.class)
      .when(dataplanePlugin)
      .resolveVersionContext(DEFAULT_VERSION);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("not found")
      .hasMessageContaining(DEFAULT_VERSION.toString())
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
    verify(userSession, never()).setSessionVersionForSource(any(), any());
  }

  @Test
  public void useBranchTypeConflictThrows() {
    // Arrange
    setUpSupportKeyAndPlugin();
    doThrow(ReferenceTypeConflictException.class)
      .when(dataplanePlugin)
      .resolveVersionContext(DEFAULT_VERSION);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("is not the requested type")
      .hasMessageContaining(DEFAULT_VERSION.toString())
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
    verify(userSession, never()).setSessionVersionForSource(any(), any());
  }

  private void setUpSupportKeyAndPlugin() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(catalog.getSource(DEFAULT_SOURCE_NAME))
      .thenReturn(dataplanePlugin);
  }

  private void setUpSupportKeyAndPluginAndSessionContext() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    when(catalog.getSource(SESSION_SOURCE_NAME))
      .thenReturn(dataplanePlugin);
    when(userSession.getDefaultSchemaPath())
      .thenReturn(new NamespaceKey(Arrays.asList(SESSION_SOURCE_NAME, "unusedFolder")));
  }

}

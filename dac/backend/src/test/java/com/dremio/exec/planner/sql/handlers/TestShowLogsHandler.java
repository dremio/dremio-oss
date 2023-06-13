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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.planner.sql.parser.ReferenceType;
import com.dremio.exec.planner.sql.parser.SqlShowLogs;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.NoDefaultBranchException;
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
 * Tests for SHOW LOGS SQL.
 */
public class TestShowLogsHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "dataplane_source_1";
  private static final String NON_EXISTENT_SOURCE_NAME = "non_exist";
  private static final String SESSION_SOURCE_NAME = "session_source";
  private static final String DEFAULT_BRANCH_NAME = "branchName";
  private static final VersionContext DEFAULT_VERSION =
    VersionContext.ofBranch(DEFAULT_BRANCH_NAME);
  private static final VersionContext SESSION_VERSION =
    VersionContext.ofBranch("session");
  private static final SqlShowLogs DEFAULT_INPUT = new SqlShowLogs(
    SqlParserPos.ZERO,
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));
  private static final SqlShowLogs NO_SOURCE_INPUT = new SqlShowLogs(
    SqlParserPos.ZERO,
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    null);
  private static final SqlShowLogs NON_EXISTENT_SOURCE_INPUT = new SqlShowLogs(
    SqlParserPos.ZERO,
    ReferenceType.BRANCH,
    new SqlIdentifier(DEFAULT_BRANCH_NAME, SqlParserPos.ZERO),
    new SqlIdentifier(NON_EXISTENT_SOURCE_NAME, SqlParserPos.ZERO));
  private static final List<ChangeInfo> EXPECTED_LOG_RESULT = Arrays.asList(
    new ChangeInfo(null, null, null, "message_1"),
      new ChangeInfo(null, null, null, "message_2"));

  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private Catalog catalog;
  @Mock private OptionManager optionManager;
  @Mock private UserSession userSession;
  @Mock private DataplanePlugin dataplanePlugin;

  @InjectMocks private ShowLogsHandler handler;

  @Test
  public void showLogsSupportKeyDisabledThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(false);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("SHOW LOGS")
      .hasMessageContaining("not supported");
  }

  @Test
  public void showLogsNonExistentSource() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX))
      .thenReturn(true);
    NamespaceNotFoundException notFoundException = new NamespaceNotFoundException("Cannot access");
    UserException nonExistException = UserException.validationError(notFoundException)
      .message("Tried to access non-existent source [%s].", NON_EXISTENT_SOURCE_NAME).build();
    when(catalog.getSource(NON_EXISTENT_SOURCE_NAME)).thenThrow(nonExistException);
    when(userSession.getSessionVersionForSource(NON_EXISTENT_SOURCE_NAME)).thenReturn(VersionContext.NOT_SPECIFIED);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NON_EXISTENT_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Tried to access non-existent source");
  }

  @Test
  public void showLogsEmptyReferenceUsesSessionVersion() throws ForemanSetupException {
    // Constants
    final SqlShowLogs input = new SqlShowLogs(
      SqlParserPos.ZERO,
      null,
      null,
      new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    when(dataplanePlugin.listChanges(SESSION_VERSION))
      .thenReturn(EXPECTED_LOG_RESULT.stream());

    // Act
    List<ChangeInfo> actualResult = handler.toResult("", input);

    // Assert
    assertThat(actualResult).isEqualTo(EXPECTED_LOG_RESULT);
  }

  @Test
  public void showLogsEmptySourceUsesSessionContext() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPluginAndSessionContext();
    when(dataplanePlugin.listChanges(DEFAULT_VERSION))
      .thenReturn(EXPECTED_LOG_RESULT.stream());

    // Act
    List<ChangeInfo> actualResult = handler.toResult("", NO_SOURCE_INPUT);

    // Assert
    assertThat(actualResult).isEqualTo(EXPECTED_LOG_RESULT);
  }

  @Test
  public void showLogsBranchSucceeds() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    when(dataplanePlugin.listChanges(DEFAULT_VERSION))
      .thenReturn(EXPECTED_LOG_RESULT.stream());

    // Act
    List<ChangeInfo> actualResult = handler.toResult("", DEFAULT_INPUT);

    // Assert
    assertThat(actualResult).isEqualTo(EXPECTED_LOG_RESULT);
  }

  @Test
  public void showLogsTagSucceeds() throws ForemanSetupException {
    // Constants
    final String tagName = "tagName";
    final VersionContext version = VersionContext.ofTag(tagName);
    final SqlShowLogs input = new SqlShowLogs(
      SqlParserPos.ZERO,
      ReferenceType.TAG,
      new SqlIdentifier(tagName, SqlParserPos.ZERO),
      new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    when(dataplanePlugin.listChanges(version))
      .thenReturn(EXPECTED_LOG_RESULT.stream());

    // Act
    List<ChangeInfo> actualResult = handler.toResult("", input);

    // Assert
    assertThat(actualResult).isEqualTo(EXPECTED_LOG_RESULT);
  }

  @Test
  public void showLogsCommitSucceeds() throws ForemanSetupException {
    // Constants
    final String commitHash = "0123456789abcdeff";
    final VersionContext version = VersionContext.ofBareCommit(commitHash);
    final SqlShowLogs input = new SqlShowLogs(
      SqlParserPos.ZERO,
      ReferenceType.COMMIT,
      new SqlIdentifier(commitHash, SqlParserPos.ZERO),
      new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    when(dataplanePlugin.listChanges(version))
      .thenReturn(EXPECTED_LOG_RESULT.stream());

    // Act
    List<ChangeInfo> actualResult = handler.toResult("", input);

    // Assert
    assertThat(actualResult).isEqualTo(EXPECTED_LOG_RESULT);
  }

  @Test
  public void showLogsReferenceSucceeds() throws ForemanSetupException {
    // Constants
    final String referenceName = "refName";
    final VersionContext version = VersionContext.ofRef(referenceName);
    final SqlShowLogs input = new SqlShowLogs(
      SqlParserPos.ZERO,
      ReferenceType.REFERENCE,
      new SqlIdentifier(referenceName, SqlParserPos.ZERO),
      new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    when(dataplanePlugin.listChanges(version))
      .thenReturn(EXPECTED_LOG_RESULT.stream());

    // Act
    List<ChangeInfo> actualResult = handler.toResult("", input);

    // Assert
    assertThat(actualResult).isEqualTo(EXPECTED_LOG_RESULT);
  }

  @Test
  public void showLogsWrongSourceThrows() {
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
  public void showLogsWrongSourceFromContextThrows() {
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
  public void showLogsNullSourceFromContextThrows() {
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
  public void showLogsNotFoundThrows() {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceNotFoundException.class)
      .when(dataplanePlugin)
      .listChanges(DEFAULT_VERSION);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("not found")
      .hasMessageContaining(DEFAULT_VERSION.toString())
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void showLogsNoDefaultBranchThrows() {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(NoDefaultBranchException.class)
      .when(dataplanePlugin)
      .listChanges(DEFAULT_VERSION);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not have a default branch")
      .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void showLogsTypeConflictThrows() {
    // Arrange
    setUpSupportKeyAndSessionVersionAndPlugin();
    doThrow(ReferenceTypeConflictException.class)
      .when(dataplanePlugin)
      .listChanges(DEFAULT_VERSION);

    // Act + Assert
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

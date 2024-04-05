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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.parser.SqlShowBranches;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundException;
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
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

/** Tests for SHOW BRANCHES SQL. */
public class TestShowBranchesHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "dataplane_source_1";
  private static final String NON_EXISTENT_SOURCE_NAME = "non_exist";
  private static final String SESSION_SOURCE_NAME = "session_source";
  private static final SqlShowBranches DEFAULT_INPUT =
      new SqlShowBranches(
          SqlParserPos.ZERO, new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));
  private static final SqlShowBranches NO_SOURCE_INPUT =
      new SqlShowBranches(SqlParserPos.ZERO, null);
  private static final SqlShowBranches NON_EXISTENT_SOURCE_INPUT =
      new SqlShowBranches(
          SqlParserPos.ZERO, new SqlIdentifier(NON_EXISTENT_SOURCE_NAME, SqlParserPos.ZERO));
  private static final List<ReferenceInfo> EXPECTED =
      Arrays.asList(
          new ReferenceInfo("Branch", "branch_1", null),
          new ReferenceInfo("Branch", "branch_2", null));

  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private OptionManager optionManager;
  @Mock private Catalog catalog;
  @Mock private UserSession userSession;
  @Mock private DataplanePlugin dataplanePlugin;

  @InjectMocks private ShowBranchesHandler handler;

  @Test
  public void showBranchesSupportKeyDisabledThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(false);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("SHOW BRANCH")
        .hasMessageContaining("not supported");
  }

  @Test
  public void showBranchesNonExistentSource() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    NamespaceNotFoundException notFoundException = new NamespaceNotFoundException("Cannot access");
    UserException nonExistException =
        UserException.validationError(notFoundException)
            .message("Tried to access non-existent source [%s].", NON_EXISTENT_SOURCE_NAME)
            .build();
    when(catalog.getSource(NON_EXISTENT_SOURCE_NAME)).thenThrow(nonExistException);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NON_EXISTENT_SOURCE_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Tried to access non-existent source");
  }

  @Test
  public void showBranchesSucceeds() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndPlugin();
    when(dataplanePlugin.listBranches()).thenReturn(EXPECTED.stream());

    // Act
    List<ReferenceInfo> result = handler.toResult("", DEFAULT_INPUT);

    // Assert
    assertThat(result).isEqualTo(EXPECTED);
  }

  @Test
  public void showBranchesNoBranchesSucceeds() throws ForemanSetupException {
    // Arrange
    setUpSupportKeyAndPlugin();
    when(dataplanePlugin.listBranches()).thenReturn(Stream.empty());
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);

    // Act
    List<ReferenceInfo> result = handler.toResult("", DEFAULT_INPUT);

    // Assert
    assertThat(result).isEmpty();
  }

  @Test
  public void showBranchesEmptySourceUsesSessionContext()
      throws ReferenceNotFoundException,
          NoDefaultBranchException,
          ReferenceConflictException,
          ForemanSetupException,
          ReferenceAlreadyExistsException {
    // Arrange
    setUpSupportKeyAndPluginAndSessionContext();
    when(dataplanePlugin.listBranches()).thenReturn(EXPECTED.stream());

    // Act
    List<ReferenceInfo> result = handler.toResult("", NO_SOURCE_INPUT);

    // Assert
    assertThat(result).isEqualTo(EXPECTED);
  }

  @Test
  public void showBranchesWrongSourceThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(mock(StoragePlugin.class));

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", DEFAULT_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("does not support")
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void showBranchesWrongSourceFromContextThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
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
  public void showBranchesNullSourceFromContextThrows() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(userSession.getDefaultSchemaPath()).thenReturn(null);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", NO_SOURCE_INPUT))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("was not specified");
  }

  private void setUpSupportKeyAndPlugin() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);
  }

  private void setUpSupportKeyAndPluginAndSessionContext() {
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalog.getSource(SESSION_SOURCE_NAME)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);
    when(userSession.getDefaultSchemaPath())
        .thenReturn(new NamespaceKey(Arrays.asList(SESSION_SOURCE_NAME, "unusedFolder")));
  }
}

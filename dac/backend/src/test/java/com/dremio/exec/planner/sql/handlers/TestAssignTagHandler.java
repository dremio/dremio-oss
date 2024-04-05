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
import static org.mockito.Mockito.any;
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
import com.dremio.exec.planner.sql.parser.ReferenceType;
import com.dremio.exec.planner.sql.parser.SqlAssignTag;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.StoragePlugin;
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

/** Tests for ALTER TAG ASSIGN. */
public class TestAssignTagHandler extends DremioTest {

  private static final String DEFAULT_SOURCE_NAME = "localnessie";
  private static final String TARGET_TAG = "tag";
  private static final String DEFAULT_REFERENCE = "reference";
  private static final VersionContext DEFAULT_VERSION = VersionContext.ofTag(TARGET_TAG);

  private OptionManager optionManager;
  private Catalog catalog;
  private AssignTagHandler handler;
  private SqlAssignTag assignTag;
  private SqlAssignTag assignTagToItself;
  private SqlAssignTag assignTagWithDefaultSource;
  private SqlAssignTag assignTagWithBranch;

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
    when(userSession.getDefaultSchemaPath())
        .thenReturn(new NamespaceKey(Arrays.asList(DEFAULT_SOURCE_NAME, "unusedFolder")));
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME)).thenReturn(DEFAULT_VERSION);
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalog.getSource(anyString())).thenReturn(dataplanePlugin);
    when(dataplanePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(dataplanePlugin.unwrap(VersionedPlugin.class)).thenReturn(dataplanePlugin);

    handler = new AssignTagHandler(context);

    assignTag =
        new SqlAssignTag(
            SqlParserPos.ZERO,
            new SqlIdentifier(TARGET_TAG, SqlParserPos.ZERO),
            ReferenceType.TAG,
            new SqlIdentifier(DEFAULT_REFERENCE, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    assignTagToItself =
        new SqlAssignTag(
            SqlParserPos.ZERO,
            new SqlIdentifier(TARGET_TAG, SqlParserPos.ZERO),
            ReferenceType.TAG,
            new SqlIdentifier(TARGET_TAG, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    assignTagWithBranch =
        new SqlAssignTag(
            SqlParserPos.ZERO,
            new SqlIdentifier(TARGET_TAG, SqlParserPos.ZERO),
            ReferenceType.BRANCH,
            new SqlIdentifier(DEFAULT_REFERENCE, SqlParserPos.ZERO),
            null,
            new SqlIdentifier(DEFAULT_SOURCE_NAME, SqlParserPos.ZERO));

    assignTagWithDefaultSource =
        new SqlAssignTag(
            SqlParserPos.ZERO,
            new SqlIdentifier(TARGET_TAG, SqlParserPos.ZERO),
            ReferenceType.TAG,
            new SqlIdentifier(DEFAULT_REFERENCE, SqlParserPos.ZERO),
            null,
            null);
  }

  @Test
  public void assignTagSucceed()
      throws ForemanSetupException, ReferenceConflictException, ReferenceNotFoundException {
    // Arrange
    doNothing().when(dataplanePlugin).assignTag(anyString(), any());

    // Assert
    List<SimpleCommandResult> result = handler.toResult("", assignTag);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);
    assertThat(result.get(0).summary)
        .contains("Assigned")
        .contains(TARGET_TAG)
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignTagThrowUnsupport() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(false);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", assignTag)).isInstanceOf(UserException.class);
  }

  @Test
  public void assignTagThrowWrongPlugin() {
    // Arrange
    when(catalog.getSource(anyString())).thenReturn(null);

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", assignTag))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("does not support")
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignTagThrowWrongSource() {
    // Arrange
    when(optionManager.getOption(ENABLE_USE_VERSION_SYNTAX)).thenReturn(true);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(mock(StoragePlugin.class));

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", assignTag))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("does not support")
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignTagThrowNotFound()
      throws ReferenceConflictException, ReferenceNotFoundException {
    // Arrange
    doThrow(ReferenceNotFoundException.class).when(dataplanePlugin).assignTag(anyString(), any());

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", assignTag))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found")
        .hasMessageContaining(DEFAULT_VERSION.toString())
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignTagThrowConflict()
      throws ReferenceConflictException, ReferenceNotFoundException {
    // Arrange
    doThrow(ReferenceConflictException.class).when(dataplanePlugin).assignTag(anyString(), any());

    // Act + Assert
    assertThatThrownBy(() -> handler.toResult("", assignTag))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("hash change")
        .hasMessageContaining(DEFAULT_VERSION.toString())
        .hasMessageContaining(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignTagWithDefaultSource()
      throws ReferenceNotFoundException, ReferenceConflictException, ForemanSetupException {
    // Arrange
    doNothing().when(dataplanePlugin).assignTag(anyString(), any());

    // Act
    List<SimpleCommandResult> result = handler.toResult("", assignTagWithDefaultSource);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("Assigned")
        .contains(DEFAULT_REFERENCE)
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignTagWithBranch()
      throws ReferenceNotFoundException, ReferenceConflictException, ForemanSetupException {
    // Arrange
    doNothing().when(dataplanePlugin).assignTag(anyString(), any());

    // Act
    List<SimpleCommandResult> result = handler.toResult("", assignTagWithBranch);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("Assigned")
        .contains("branch")
        .contains(DEFAULT_REFERENCE)
        .contains(DEFAULT_SOURCE_NAME);
  }

  @Test
  public void assignTagToItself()
      throws ReferenceNotFoundException, ReferenceConflictException, ForemanSetupException {
    // Arrange
    doNothing().when(dataplanePlugin).assignTag(anyString(), any());

    // Act
    List<SimpleCommandResult> result = handler.toResult("", assignTagToItself);

    // Assert
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
        .contains("Assigned")
        .contains("tag")
        .contains(TARGET_TAG)
        .contains(DEFAULT_SOURCE_NAME);
  }
}

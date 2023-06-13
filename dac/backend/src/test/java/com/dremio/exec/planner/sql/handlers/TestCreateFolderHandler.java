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

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.model.ContentKey;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.ReferenceType;
import com.dremio.exec.planner.sql.parser.SqlCreateFolder;
import com.dremio.exec.planner.sql.parser.SqlGrant;
import com.dremio.exec.store.NessieNamespaceAlreadyExistsException;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.plugins.s3.store.S3StoragePlugin;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.test.DremioTest;

@ExtendWith(MockitoExtension.class)
public class TestCreateFolderHandler extends DremioTest {
  private static final String DEFAULT_CONTEXT = "@dremio";
  private static final String NON_EXISTENT_SOURCE_NAME = "non_exist";
  private static final String DEFAULT_SOURCE_NAME = "dataplane_source_1";
  private static final String NON_VERSIONED_SOURCE_NAME = "s3Source";
  private static final String DEFAULT_FOLDER_NAME = "myFolder";
  private static final List<String> SINGLE_FOLDER_PATH = Arrays.asList(DEFAULT_FOLDER_NAME);
  private static final List<String> NON_VERSIONED_SOURCE_PATH = Arrays.asList(NON_VERSIONED_SOURCE_NAME, DEFAULT_FOLDER_NAME);
  private static final List<String> DEFAULT_FOLDER_PATH = Arrays.asList(DEFAULT_SOURCE_NAME, DEFAULT_FOLDER_NAME);
  private static final List<String> NON_EXISTENT_FOLDER_PATH = Arrays.asList(NON_EXISTENT_SOURCE_NAME, DEFAULT_FOLDER_NAME);
  private static final String DEV_BRANCH_NAME= "dev";

  private static final VersionContext DEV_VERSION =
    VersionContext.ofBranch("dev");

  @Mock private Catalog catalog;
  @Mock private UserSession userSession;
  @Mock private DataplanePlugin dataplanePlugin;
  @Mock private S3StoragePlugin s3StoragePlugin;
  @InjectMocks private CreateFolderHandler handler;

  private static final SqlCreateFolder NON_EXISTENT_SOURCE_INPUT = new SqlCreateFolder(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(NON_EXISTENT_FOLDER_PATH, SqlParserPos.ZERO),
    null,
    null);

  private static final SqlCreateFolder DEFAULT_SOURCE_INPUT = new SqlCreateFolder(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_FOLDER_PATH, SqlParserPos.ZERO),
    null,
    null);

  private static final SqlCreateFolder SINGLE_FOLDER_NAME_NO_USER_SESSION_INPUT = new SqlCreateFolder(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(SINGLE_FOLDER_PATH, SqlParserPos.ZERO),
    null,
    null);

  private static final SqlCreateFolder SINGLE_FOLDER_NAME_WITH_USER_SESSION_INPUT = new SqlCreateFolder(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(SINGLE_FOLDER_PATH, SqlParserPos.ZERO),
    null,
    null);

  private static final SqlCreateFolder NON_VERSIONED_SOURCE_INPUT = new SqlCreateFolder(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(NON_VERSIONED_SOURCE_PATH, SqlParserPos.ZERO),
    null,
    null);

  private static final SqlCreateFolder WITH_REFERENCE_INPUT = new SqlCreateFolder(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_FOLDER_PATH, SqlParserPos.ZERO),
    ReferenceType.BRANCH,
    new SqlIdentifier(DEV_BRANCH_NAME, SqlParserPos.ZERO));

  private static final SqlCreateFolder WITH_IF_NOT_EXISTS = new SqlCreateFolder(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_FOLDER_PATH, SqlParserPos.ZERO),
    ReferenceType.BRANCH,
    new SqlIdentifier(DEV_BRANCH_NAME, SqlParserPos.ZERO));

  private static final SqlCreateFolder WITHOUT_IF_NOT_EXISTS = new SqlCreateFolder(
    SqlParserPos.ZERO,
    SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
    new SqlIdentifier(DEFAULT_FOLDER_PATH, SqlParserPos.ZERO),
    ReferenceType.BRANCH,
    new SqlIdentifier(DEV_BRANCH_NAME, SqlParserPos.ZERO));

  /**
   * CREATE FOLDER SQL SYNTAX
   * CREATE FOLDER [ IF NOT EXISTS ] [source.]parentFolderName[.childFolder]
   * [ AT ( REF[ERENCE) | BRANCH | TAG | COMMIT ) refValue ]
   */
  @Test
  public void createFolderOnNonExistentSource() throws Exception{
    NamespaceNotFoundException namespaceNotFoundException = new NamespaceNotFoundException("Cannot access");
    UserException nonExistUserException = UserException.validationError(namespaceNotFoundException)
      .message("Tried to access non-existent source [%s].", NON_EXISTENT_SOURCE_NAME).build();
    when(userSession.getSessionVersionForSource(NON_EXISTENT_SOURCE_NAME)).thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getSource(NON_EXISTENT_SOURCE_NAME)).thenThrow(nonExistUserException);
    when(catalog.resolveSingle(extractNamespaceKeyFromSqlNode(NON_EXISTENT_SOURCE_INPUT))).thenReturn(extractNamespaceKeyFromSqlNode(NON_EXISTENT_SOURCE_INPUT));
    assertThatThrownBy(() -> handler.toResult("", NON_EXISTENT_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Tried to access non-existent source");
    NamespaceKey path = SqlNodeUtil.unwrap(NON_EXISTENT_SOURCE_INPUT, SqlCreateFolder.class).getPath();
    verify(catalog).resolveSingle(path);
    verify(catalog).validatePrivilege(path, SqlGrant.Privilege.ALTER);
  }

  @Test
  public void createFolderInExistentSource() throws Exception {
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME)).thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(dataplanePlugin);
    when(catalog.resolveSingle(extractNamespaceKeyFromSqlNode(DEFAULT_SOURCE_INPUT))).thenReturn(extractNamespaceKeyFromSqlNode(DEFAULT_SOURCE_INPUT));

    List<SimpleCommandResult> result;
    result = handler.toResult("", DEFAULT_SOURCE_INPUT);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("Folder")
      .contains("has been created");
    NamespaceKey path = SqlNodeUtil.unwrap(DEFAULT_SOURCE_INPUT, SqlCreateFolder.class).getPath();
    verify(catalog).resolveSingle(path);
    verify(catalog).validatePrivilege(path, SqlGrant.Privilege.ALTER);
  }

  @Test
  public void createFolderInExistentSourceWithSingleFolderNameWithoutUserSession() throws Exception {
    NamespaceNotFoundException namespaceNotFoundException = new NamespaceNotFoundException("Cannot access");
    UserException nonExistUserException = UserException.validationError(namespaceNotFoundException)
      .message("Tried to access non-existent source [%s].", NON_EXISTENT_SOURCE_NAME).build();
    when(userSession.getSessionVersionForSource(DEFAULT_CONTEXT)).thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getSource(DEFAULT_CONTEXT)).thenThrow(nonExistUserException);
    when(catalog.resolveSingle(extractNamespaceKeyFromSqlNode(SINGLE_FOLDER_NAME_NO_USER_SESSION_INPUT))).thenReturn(new NamespaceKey(Arrays.asList(DEFAULT_CONTEXT, DEFAULT_FOLDER_NAME)));

    assertThatThrownBy(() -> handler.toResult("", SINGLE_FOLDER_NAME_NO_USER_SESSION_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Tried to access non-existent source");
    NamespaceKey path = new NamespaceKey(Arrays.asList(DEFAULT_CONTEXT, DEFAULT_FOLDER_NAME));
    verify(catalog).resolveSingle(extractNamespaceKeyFromSqlNode(SINGLE_FOLDER_NAME_NO_USER_SESSION_INPUT));
    verify(catalog).validatePrivilege(path, SqlGrant.Privilege.ALTER);
  }

  @Test
  public void createFolderInExistentSourceWithSingleFolderNameWithUserSession() throws Exception {
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME)).thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(dataplanePlugin);
    when(catalog.resolveSingle(extractNamespaceKeyFromSqlNode(SINGLE_FOLDER_NAME_WITH_USER_SESSION_INPUT))).thenReturn(new NamespaceKey(Arrays.asList(DEFAULT_SOURCE_NAME, DEFAULT_FOLDER_NAME)));

    List<SimpleCommandResult> result;
    result =  handler.toResult("", SINGLE_FOLDER_NAME_WITH_USER_SESSION_INPUT);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("Folder")
      .contains("has been created");
    NamespaceKey path = new NamespaceKey(Arrays.asList(DEFAULT_SOURCE_NAME, DEFAULT_FOLDER_NAME));
    verify(catalog).resolveSingle(extractNamespaceKeyFromSqlNode(SINGLE_FOLDER_NAME_WITH_USER_SESSION_INPUT));
    verify(catalog).validatePrivilege(path, SqlGrant.Privilege.ALTER);
  }

  @Test
  public void createFolderInNonVersionedSource() throws Exception {
    when(userSession.getSessionVersionForSource(NON_VERSIONED_SOURCE_NAME)).thenReturn(VersionContext.NOT_SPECIFIED);
    when(catalog.getSource(NON_VERSIONED_SOURCE_NAME)).thenReturn(s3StoragePlugin);
    when(catalog.resolveSingle(extractNamespaceKeyFromSqlNode(NON_VERSIONED_SOURCE_INPUT))).thenReturn(extractNamespaceKeyFromSqlNode(NON_VERSIONED_SOURCE_INPUT));

    assertThatThrownBy(() -> handler.toResult("", NON_VERSIONED_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support versioning");
    NamespaceKey path = SqlNodeUtil.unwrap(NON_VERSIONED_SOURCE_INPUT, SqlCreateFolder.class).getPath();
    verify(catalog).resolveSingle(path);
    verify(catalog).validatePrivilege(path, SqlGrant.Privilege.ALTER);
  }

  @Test
  public void createFolderWithReference() throws Exception {
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME))
      .thenReturn(DEV_VERSION);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(dataplanePlugin);
    when(catalog.resolveSingle(extractNamespaceKeyFromSqlNode(WITH_REFERENCE_INPUT))).thenReturn(extractNamespaceKeyFromSqlNode(WITH_REFERENCE_INPUT));

    List<SimpleCommandResult> result;
    result =  handler.toResult("", WITH_REFERENCE_INPUT);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("Folder")
      .contains("has been created")
      .contains(String.format("created at branch %s", DEV_BRANCH_NAME));
    NamespaceKey path = SqlNodeUtil.unwrap(WITH_REFERENCE_INPUT, SqlCreateFolder.class).getPath();
    verify(catalog).resolveSingle(path);
    verify(catalog).validatePrivilege(path, SqlGrant.Privilege.ALTER);
  }

  @Test
  public void createFolderWithIfNotExists() throws Exception {
    ContentKey contentKey = ContentKey.of(WITHOUT_IF_NOT_EXISTS.getPath().getPathComponents());
    NessieNamespaceAlreadyExistsException nessieNamespaceAlreadyExistsException = new NessieNamespaceAlreadyExistsException(String.format("Folder %s already exists", contentKey.toPathString()));
    when(catalog.resolveSingle(extractNamespaceKeyFromSqlNode(WITHOUT_IF_NOT_EXISTS))).thenReturn(extractNamespaceKeyFromSqlNode(WITH_IF_NOT_EXISTS));
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME))
      .thenReturn(DEV_VERSION);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(dataplanePlugin);
    doThrow(nessieNamespaceAlreadyExistsException)
      .when(dataplanePlugin)
      .createNamespace(new NamespaceKey(DEFAULT_FOLDER_PATH), DEV_VERSION);

    List<SimpleCommandResult> result;
    result =  handler.toResult("", WITH_IF_NOT_EXISTS);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary)
      .contains("Folder")
      .contains("already exists");
    NamespaceKey path = SqlNodeUtil.unwrap(WITH_IF_NOT_EXISTS, SqlCreateFolder.class).getPath();
    verify(catalog).resolveSingle(path);
    verify(catalog).validatePrivilege(path, SqlGrant.Privilege.ALTER);
  }

  @Test
  public void createFolderWithoutIfNotExists() throws Exception {
    ContentKey contentKey = ContentKey.of(WITHOUT_IF_NOT_EXISTS.getPath().getPathComponents());
    NessieNamespaceAlreadyExistsException nessieNamespaceAlreadyExistsException = new NessieNamespaceAlreadyExistsException(String.format("Folder %s already exists", contentKey.toPathString()));
    when(catalog.resolveSingle(extractNamespaceKeyFromSqlNode(WITHOUT_IF_NOT_EXISTS))).thenReturn(extractNamespaceKeyFromSqlNode(WITHOUT_IF_NOT_EXISTS));
    when(userSession.getSessionVersionForSource(DEFAULT_SOURCE_NAME))
      .thenReturn(DEV_VERSION);
    when(catalog.getSource(DEFAULT_SOURCE_NAME)).thenReturn(dataplanePlugin);
    doThrow(nessieNamespaceAlreadyExistsException)
      .when(dataplanePlugin)
      .createNamespace(new NamespaceKey(DEFAULT_FOLDER_PATH), DEV_VERSION);

    assertThatThrownBy(() -> handler.toResult("", WITHOUT_IF_NOT_EXISTS))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Folder")
      .hasMessageContaining("already exists");
    NamespaceKey path = SqlNodeUtil.unwrap(WITHOUT_IF_NOT_EXISTS, SqlCreateFolder.class).getPath();
    verify(catalog).resolveSingle(path);
    verify(catalog).validatePrivilege(path, SqlGrant.Privilege.ALTER);
  }


  @Test
  public void createFolderWithoutALTERPrivilege() throws Exception {
    doThrow(UserException.validationError().message("permission denied").buildSilently())
      .when(catalog)
      .validatePrivilege(DEFAULT_SOURCE_INPUT.getPath(), SqlGrant.Privilege.ALTER);
    when(catalog.resolveSingle(extractNamespaceKeyFromSqlNode(DEFAULT_SOURCE_INPUT))).thenReturn(extractNamespaceKeyFromSqlNode(DEFAULT_SOURCE_INPUT));

    assertThatThrownBy(() -> handler.toResult("", DEFAULT_SOURCE_INPUT))
      .isInstanceOf(UserException.class)
      .hasMessage("permission denied");
    NamespaceKey path = SqlNodeUtil.unwrap(DEFAULT_SOURCE_INPUT, SqlCreateFolder.class).getPath();
    verify(catalog).resolveSingle(path);
    verify(catalog).validatePrivilege(path, SqlGrant.Privilege.ALTER);
  }

  private NamespaceKey extractNamespaceKeyFromSqlNode(SqlNode sqlNode) throws Exception{
    return requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlCreateFolder.class)).getPath();
  }
}

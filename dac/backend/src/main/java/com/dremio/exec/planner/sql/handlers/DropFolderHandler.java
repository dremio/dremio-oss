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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlNode;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlDropFolder;
import com.dremio.exec.store.NessieNamespaceNotEmptyException;
import com.dremio.exec.store.NessieNamespaceNotFoundException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;

public class DropFolderHandler extends BaseVersionHandler<SimpleCommandResult> {

  private final UserSession userSession;

  public DropFolderHandler(Catalog catalog, UserSession userSession) {
    super(catalog);
    this.userSession = requireNonNull(userSession);
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {

    final SqlDropFolder dropFolder = requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlDropFolder.class));
    NamespaceKey path = dropFolder.getPath();
    String sourceName = path.getRoot();
    // since the path has single item, we add context.
    if (path.getPathComponents().size() == 1) {
      sourceName = userSession.getDefaultSchemaName();
      path = new NamespaceKey(Arrays.asList(sourceName, path.getName()));
    }

    final boolean existenceCheck = dropFolder.getExistenceCheck().booleanValue();
    VersionContext statementSourceVersion =
      ReferenceTypeUtils.map(dropFolder.getRefType(), dropFolder.getRefValue(), null);
    VersionContext sessionVersion = userSession.getSessionVersionForSource(sourceName);
    VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);

    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceName);

    try{
      versionedPlugin.deleteFolder(path, sourceVersion);
    } catch (NessieNamespaceNotFoundException e) {
      if (existenceCheck) {
        return Collections.singletonList(
          SimpleCommandResult.successful(e.getMessage()));
      }
      throw UserException.validationError(e)
        .message(e.getMessage())
        .buildSilently();
    }  catch (NessieNamespaceNotEmptyException e) {
      throw UserException.validationError(e)
        .message(e.getMessage())
        .buildSilently();
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
        .message("Source %s not found in Source %s.", sourceVersion, sourceName)
        .buildSilently();
    } catch (NoDefaultBranchException e) {
      throw UserException.validationError(e)
        .message("Unable to resolve source version. Version was not specified and Source %s does not have a default branch set.", sourceName)
        .buildSilently();
    } catch (ReferenceTypeConflictException e) {
      throw UserException.validationError(e)
        .message("Requested %s in source %s is not the requested type.", sourceVersion, sourceName)
        .buildSilently();
    }

    String sourceVersionMessage = sourceVersion.isSpecified()
      ? sourceVersion.toString()
      : "the default branch";
    return Collections.singletonList(
      SimpleCommandResult.successful(
        "Folder %s has been deleted at %s in source %s.",
        path.getName(),
        sourceVersionMessage,
        sourceName));
  }

  @Override
  public Class<SimpleCommandResult> getResultType() {
    return SimpleCommandResult.class;
  }
}

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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlCreateBranch;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceAlreadyExistsException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Handler for creating a branch.
 *
 * CREATE BRANCH [ IF NOT EXISTS ] branchName
 * [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue [AS OF timestamp] ]
 * [ IN sourceName ]
 */
public class CreateBranchHandler extends BaseVersionHandler<SimpleCommandResult> {
  private final UserSession userSession;

  public CreateBranchHandler(Catalog catalog, OptionResolver optionResolver, UserSession userSession) {
    super(catalog, optionResolver);
    this.userSession = requireNonNull(userSession);
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode)
      throws ForemanSetupException {
    checkFeatureEnabled("CREATE BRANCH syntax is not supported.");

    final SqlCreateBranch createBranch = requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlCreateBranch.class));
    final SqlIdentifier sourceIdentifier = createBranch.getSourceName();
    final String sourceName = VersionedHandlerUtils.resolveSourceName(
      sourceIdentifier,
      userSession.getDefaultSchemaPath());

    final boolean shouldErrorIfVersionExists = createBranch.shouldErrorIfVersionExists().booleanValue();
    final String branchName = requireNonNull(createBranch.getBranchName()).toString();

    VersionContext statementSourceVersion =
      ReferenceTypeUtils.map(createBranch.getRefType(), createBranch.getRefValue(), createBranch.getTimestamp());
    VersionContext sessionVersion = userSession.getSessionVersionForSource(sourceName);
    VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);

    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceName);
    try {
      versionedPlugin.createBranch(branchName, sourceVersion);
    } catch (ReferenceAlreadyExistsException e) {
      if (shouldErrorIfVersionExists) {
        throw UserException.validationError(e)
            .message(HandlerUtils.REFERENCE_ALREADY_EXISTS_MESSAGE, branchName, sourceName)
            .buildSilently();
      }
      return Collections.singletonList(
        SimpleCommandResult.successful(
          HandlerUtils.REFERENCE_ALREADY_EXISTS_MESSAGE,
          branchName,
          sourceName));
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
            "Branch %s has been created at %s in source %s.",
            branchName,
            sourceVersionMessage,
            sourceName));
  }

  @Override
  public Class<SimpleCommandResult> getResultType() {
    return SimpleCommandResult.class;
  }
}

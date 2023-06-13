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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlDropBranch;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.base.Strings;

/**
 * Handler for dropping branch.
 *
 * DROP BRANCH [ IF EXISTS ] branchName
 * ( AT COMMIT commitHash | FORCE )
 * [ IN sourceName ]
 */
public class DropBranchHandler extends BaseVersionHandler<SimpleCommandResult> {

  private final UserSession userSession;

  public DropBranchHandler(Catalog catalog, OptionResolver optionResolver, UserSession userSession) {
    super(catalog, optionResolver);
    this.userSession = requireNonNull(userSession);
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode)
      throws ForemanSetupException {
    checkFeatureEnabled("DROP BRANCH syntax is not supported.");

    final SqlDropBranch dropBranch = requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlDropBranch.class));
    final SqlIdentifier sourceIdentifier = dropBranch.getSourceName();
    final String sourceName = VersionedHandlerUtils.resolveSourceName(
      sourceIdentifier,
      userSession.getDefaultSchemaPath());

    String commitHash = (dropBranch.getCommitHash() != null)
      ? dropBranch.getCommitHash().toString()
      : ""; // Will imply force drop
    final String branchName = requireNonNull(dropBranch.getBranchName()).toString();
    final boolean forceDrop = dropBranch.getForceDrop().booleanValue();
    final boolean existenceCheck = dropBranch.getExistenceCheck().booleanValue();

    if (!forceDrop && Strings.isNullOrEmpty(commitHash)) {
      // This shouldn't be possible, enforced by SQL parser
      throw UserException.validationError()
          .message("Need commit hash to drop branch %s on source %s.", branchName, sourceName)
          .buildSilently();
    }

    //Prevent dropping current branch
    VersionContext currentSessionVersion = userSession.getSessionVersionForSource(sourceName);
    if (currentSessionVersion.isBranch() && currentSessionVersion.getValue().equals(branchName)){
      throw UserException.validationError()
        .message("Cannot drop branch %s for source %s while it is set in the current session's reference context ", branchName, sourceName)
        .buildSilently();
    }

    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceName);
    try {
      versionedPlugin.dropBranch(branchName, commitHash);
    } catch (ReferenceConflictException e) {
      // TODO: DX-43145 Retries if forceDrop is true?
      throw UserException.validationError(e)
          .message("Branch %s has conflict on source %s.", branchName, sourceName)
          .buildSilently();
    } catch (ReferenceNotFoundException e) {
      if (existenceCheck) {
        throw UserException.validationError(e)
            .message("Branch %s not found on source %s.", branchName, sourceName)
            .buildSilently();
      }
      // Return success, but still give message about not found
      return Collections.singletonList(
        SimpleCommandResult.successful(
          "Branch %s not found on source %s.", branchName, sourceName));
    }

    return Collections.singletonList(
        SimpleCommandResult.successful(
            "Branch %s has been dropped on source %s.", branchName, sourceName));
  }

  @Override
  public Class<SimpleCommandResult> getResultType() {
    return SimpleCommandResult.class;
  }
}

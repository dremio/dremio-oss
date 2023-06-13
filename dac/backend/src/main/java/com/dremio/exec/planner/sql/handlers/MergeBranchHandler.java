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
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlMergeBranch;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.sabot.rpc.user.UserSession;

/** Handler for merging branch. */
public class MergeBranchHandler extends BaseVersionHandler<SimpleCommandResult> {
  private final UserSession userSession;

  public MergeBranchHandler(QueryContext context) {
    super(context.getCatalog(), context.getOptions());
    this.userSession = requireNonNull(context.getSession());
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode)
      throws ForemanSetupException {
    checkFeatureEnabled("MERGE BRANCH syntax is not supported.");

    final SqlMergeBranch mergeBranch = requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlMergeBranch.class));
    final SqlIdentifier sourceIdentifier = mergeBranch.getSourceName();
    final String sourceName = VersionedHandlerUtils.resolveSourceName(
      sourceIdentifier,
      userSession.getDefaultSchemaPath());

    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceName);
    final String sourceBranchName = requireNonNull(mergeBranch.getSourceBranchName()).toString();
    final VersionContext sessionVersion = userSession.getSessionVersionForSource(sourceName);

    String targetBranchName;
    if (mergeBranch.getTargetBranchName() != null) {
      targetBranchName = mergeBranch.getTargetBranchName().toString();
    } else if (sessionVersion.isBranch()) {
      targetBranchName = sessionVersion.getValue();
    } else {
      throw UserException.validationError()
          .message("No target branch to merge into.")
          .buildSilently();
    }

    try {
      versionedPlugin.mergeBranch(sourceBranchName, targetBranchName);
    } catch (ReferenceConflictException e) {
      throw UserException.validationError(e)
          .message(
              "Merge branch %s into branch %s failed due to commit conflict on source %s.",
              sourceBranchName, targetBranchName, sourceName)
          .buildSilently();
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
          .message(
              "Merge branch %s into branch %s failed due to missing branch on source %s.",
              sourceBranchName, targetBranchName, sourceName)
          .buildSilently();
    }

    return Collections.singletonList(
        SimpleCommandResult.successful(
            "Branch %s has been merged into %s on source %s.",
            sourceBranchName, targetBranchName, sourceName));
  }

  @Override
  public Class<SimpleCommandResult> getResultType() {
    return SimpleCommandResult.class;
  }
}

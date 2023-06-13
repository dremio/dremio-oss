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
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlAssignTag;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Handler for updating the reference to the given tag.
 *
 * ALTER TAG tagName ASSIGN
 * ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue
 * [ IN sourceName ]
 */
public class AssignTagHandler extends BaseVersionHandler<SimpleCommandResult> {
  private final UserSession userSession;

  public AssignTagHandler(QueryContext context) {
    super(context.getCatalog(), context.getOptions());
    this.userSession = requireNonNull(context.getSession());
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode)
      throws ForemanSetupException {
    checkFeatureEnabled("ALTER TAG ASSIGN syntax is not supported.");

    final SqlAssignTag assignTag =
       requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlAssignTag.class));
    final SqlIdentifier sourceIdentifier = assignTag.getSourceName();
    final String sourceName =  VersionedHandlerUtils.resolveSourceName(
      sourceIdentifier,
      userSession.getDefaultSchemaPath());

    final VersionContext statementVersion =
      ReferenceTypeUtils.map(assignTag.getRefType(), assignTag.getRefValue());
    final String tagName = requireNonNull(assignTag.getTagName()).toString();

    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceName);
    try {
      versionedPlugin.assignTag(tagName, statementVersion);
    } catch (ReferenceConflictException e) {
      throw UserException.validationError(e)
          .message(
              "Assign %s to tag %s on source %s failed with hash change.",
              statementVersion, tagName, sourceName)
          .buildSilently();
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
          .message(
              "Assign %s to tag %s on source %s failed with not found.",
              statementVersion, tagName, sourceName)
          .buildSilently();
    }

    return Collections.singletonList(
        SimpleCommandResult.successful(
            "Assigned %s to tag %s on source %s.",
            statementVersion, tagName, sourceName));
  }

  @Override
  public Class<SimpleCommandResult> getResultType() {
    return SimpleCommandResult.class;
  }
}

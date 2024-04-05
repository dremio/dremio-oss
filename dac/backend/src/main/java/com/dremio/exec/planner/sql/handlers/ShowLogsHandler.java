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

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlShowLogs;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

/**
 * Handler to show logs for specific ref.
 *
 * <p>SHOW LOGS [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue [AS OF timestamp] ] [ IN
 * sourceName ]
 */
public class ShowLogsHandler extends BaseVersionHandler<ChangeInfo> {
  private final UserSession userSession;

  public ShowLogsHandler(Catalog catalog, OptionResolver optionResolver, UserSession userSession) {
    super(catalog, optionResolver);
    this.userSession = requireNonNull(userSession);
  }

  @Override
  public List<ChangeInfo> toResult(String sql, SqlNode sqlNode) throws ForemanSetupException {
    checkFeatureEnabled("SHOW LOGS syntax is not supported.");

    final SqlShowLogs showLogs = requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlShowLogs.class));
    final SqlIdentifier sourceIdentifier = showLogs.getSourceName();
    final String sourceName =
        VersionedHandlerUtils.resolveSourceName(
            sourceIdentifier, userSession.getDefaultSchemaPath());

    VersionContext statementVersion =
        ReferenceTypeUtils.map(
            showLogs.getRefType(), showLogs.getRefValue(), showLogs.getTimestamp());
    final VersionContext sessionVersion = userSession.getSessionVersionForSource(sourceName);
    final VersionContext version = statementVersion.orElse(sessionVersion);

    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceName);
    try {
      return versionedPlugin.listChanges(version).collect(Collectors.toList());
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
          .message("Requested %s not found in source %s.", version, sourceName)
          .buildSilently();
    } catch (NoDefaultBranchException e) {
      throw UserException.validationError(e)
          .message(
              "Unable to resolve requested version. Version was not specified and Source %s does not have a default branch set.",
              sourceName)
          .buildSilently();
    } catch (ReferenceTypeConflictException e) {
      throw UserException.validationError(e)
          .message("Requested %s in source %s is not the requested type.", version, sourceName)
          .buildSilently();
    }
  }

  @Override
  public Class<ChangeInfo> getResultType() {
    return ChangeInfo.class;
  }
}

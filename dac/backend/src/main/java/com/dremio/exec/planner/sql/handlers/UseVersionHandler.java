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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlUseVersion;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

public class UseVersionHandler extends BaseVersionHandler<SimpleCommandResult> {
  private final UserSession userSession;

  public UseVersionHandler(
      Catalog catalog, UserSession userSession, OptionResolver optionResolver) {
    super(catalog, optionResolver);
    this.userSession = Preconditions.checkNotNull(userSession);
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode)
      throws ForemanSetupException {
    final SqlUseVersion useVersion =
        requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlUseVersion.class));

    checkFeatureEnabled(String.format("USE %s syntax is not supported.", useVersion.getRefType()));

    final SqlIdentifier sourceIdentifier = useVersion.getSourceName();
    final String sourceName =
        VersionedHandlerUtils.resolveSourceName(
            sourceIdentifier, userSession.getDefaultSchemaPath());

    VersionContext requestedVersion =
        ReferenceTypeUtils.map(
            useVersion.getRefType(), useVersion.getRefValue(), useVersion.getTimestamp());
    if (!requestedVersion.isSpecified()) {
      // Defensive, this shouldn't be possible
      throw new IllegalStateException("Must request a real version.");
    }

    // Validate that the requested version exists
    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceName);
    final ResolvedVersionContext resolvedVersionContext;
    try {
      resolvedVersionContext = versionedPlugin.resolveVersionContext(requestedVersion);
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
          .message("Requested %s not found in source %s.", requestedVersion, sourceName)
          .buildSilently();
    } catch (NoDefaultBranchException e) {
      // This only happens if we try to resolve VersionContext.NOT_SPECIFIED,
      // which should not be possible here.
      throw new IllegalStateException(e);
    } catch (ReferenceTypeConflictException e) {
      throw UserException.validationError(e)
          .message(
              "Requested %s in source %s is not the requested type.", requestedVersion, sourceName)
          .buildSilently();
    }

    // Resolving a bare commit does not validate existence for performance reasons. Check explicitly
    // so that we can fail early and inform the user.
    if (resolvedVersionContext.isCommit()
        && !versionedPlugin.commitExists(resolvedVersionContext.getCommitHash())) {
      throw UserException.validationError()
          .message(
              "Commit %s not found in source %s.",
              resolvedVersionContext.getCommitHash(), sourceName)
          .buildSilently();
    }

    userSession.setSessionVersionForSource(sourceName, requestedVersion);

    return Collections.singletonList(
        SimpleCommandResult.successful(
            "Current version context set to %s in source %s.", requestedVersion, sourceName));
  }

  @Override
  public Class<SimpleCommandResult> getResultType() {
    return SimpleCommandResult.class;
  }
}

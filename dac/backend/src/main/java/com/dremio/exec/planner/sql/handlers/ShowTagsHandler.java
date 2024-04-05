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

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlShowTags;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

/**
 * Handler to show source's tags.
 *
 * <p>SHOW TAGS [ IN sourceName ]
 */
public class ShowTagsHandler extends BaseVersionHandler<ReferenceInfo> {

  private final UserSession userSession;

  public ShowTagsHandler(Catalog catalog, OptionResolver optionResolver, UserSession userSession) {
    super(catalog, optionResolver);
    this.userSession = requireNonNull(userSession);
  }

  @Override
  public List<ReferenceInfo> toResult(String sql, SqlNode sqlNode) throws ForemanSetupException {
    checkFeatureEnabled("SHOW TAGS syntax is not supported.");

    final SqlShowTags showTags = requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlShowTags.class));
    final SqlIdentifier sourceIdentifier = showTags.getSourceName();
    final String sourceName =
        VersionedHandlerUtils.resolveSourceName(
            sourceIdentifier, userSession.getDefaultSchemaPath());

    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceName);
    return versionedPlugin.listTags().collect(Collectors.toList());
  }

  @Override
  public Class<ReferenceInfo> getResultType() {
    return ReferenceInfo.class;
  }
}

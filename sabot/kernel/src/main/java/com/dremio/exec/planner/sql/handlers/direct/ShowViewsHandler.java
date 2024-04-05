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
package com.dremio.exec.planner.sql.handlers.direct;

import static com.dremio.exec.planner.sql.handlers.direct.ShowHandlerUtil.checkVersionedFeatureEnabled;
import static com.dremio.exec.planner.sql.handlers.direct.ShowHandlerUtil.concatSourceNameAndNamespace;
import static com.dremio.exec.planner.sql.handlers.direct.ShowHandlerUtil.getVersionedPlugin;
import static com.dremio.exec.planner.sql.handlers.direct.ShowHandlerUtil.validate;
import static java.util.Objects.requireNonNull;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.sql.handlers.VersionedHandlerUtils;
import com.dremio.exec.planner.sql.parser.ReferenceTypeUtils;
import com.dremio.exec.planner.sql.parser.SqlShowViews;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlNode;

public class ShowViewsHandler implements SqlDirectHandler<ShowViewsHandler.ShowViewResult> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ShowViewsHandler.class);

  private final Catalog catalog;
  private final OptionResolver optionResolver;
  private final UserSession userSession;

  public ShowViewsHandler(Catalog catalog, OptionResolver optionResolver, UserSession userSession) {
    this.catalog = catalog;
    this.optionResolver = optionResolver;
    this.userSession = userSession;
  }

  @Override
  public List<ShowViewResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlShowViews sqlShowViews = SqlNodeUtil.unwrap(sqlNode, SqlShowViews.class);

    final NamespaceKey sourcePath =
        VersionedHandlerUtils.resolveSourceNameAsNamespaceKey(
            sqlShowViews.getSourceName(), userSession.getDefaultSchemaPath());
    final String sourceName = sourcePath.getRoot();
    VersionContext statementSourceVersion =
        ReferenceTypeUtils.map(
            sqlShowViews.getRefType(), sqlShowViews.getRefValue(), sqlShowViews.getTimestamp());
    final VersionContext sessionVersion =
        userSession.getSessionVersionForSource(sourceName).orElse(VersionContext.NOT_SPECIFIED);
    final VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);
    CatalogEntityKey sourceKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(sourcePath.getPathComponents())
            .tableVersionContext(TableVersionContext.of(sourceVersion))
            .build();
    validate(sourceKey, catalog);

    if (CatalogUtil.requestedPluginSupportsVersionedTables(sourcePath, catalog)) {
      return showVersionedViews(
          sqlNode,
          CatalogEntityKey.newBuilder()
              .keyComponents(sourcePath.getPathComponents())
              .tableVersionContext(TableVersionContext.of(sourceVersion))
              .build());
    } else {
      throw UserException.unsupportedError()
          .message("Source %s does not support show views.", sourceName)
          .build(logger);
    }
  }

  @Override
  public Class<ShowViewResult> getResultType() {
    return ShowViewResult.class;
  }

  private List<ShowViewResult> showVersionedViews(SqlNode sqlNode, CatalogEntityKey sourcePath)
      throws ForemanSetupException {
    checkVersionedFeatureEnabled(optionResolver, "SHOW VIEWS syntax is not supported.");

    final SqlShowViews showViews = requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlShowViews.class));

    final Pattern likePattern = SqlNodeUtil.getPattern(showViews.getLikePattern());
    final Matcher m = likePattern.matcher("");
    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourcePath.getRootEntity(), catalog);
    final List<String> path = sourcePath.getPathWithoutRoot();
    try {
      return versionedPlugin
          .listViewsIncludeNested(path, sourcePath.getTableVersionContext().asVersionContext())
          .filter(view -> m.reset(view.getName()).matches())
          .map(
              entry ->
                  new ShowViewResult(
                      concatSourceNameAndNamespace(
                          sourcePath.getRootEntity(), entry.getNamespace()),
                      entry.getName()))
          .collect(Collectors.toList());
    } catch (ReferenceConflictException e) {
      throw UserException.validationError(e)
          .message(
              "%s has conflict on source %s.",
              sourcePath.getTableVersionContext().asVersionContext(), sourcePath.getRootEntity())
          .buildSilently();
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
          .message(
              "%s not found on source %s.",
              sourcePath.getTableVersionContext().asVersionContext(), sourcePath.getRootEntity())
          .buildSilently();
    }
  }

  public static class ShowViewResult {
    public final String VIEW_PATH;
    public final String VIEW_NAME;

    public ShowViewResult(String viewPath, String viewName) {
      super();
      VIEW_PATH = viewPath;
      VIEW_NAME = viewName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ShowViewsHandler.ShowViewResult that = (ShowViewsHandler.ShowViewResult) o;
      return Objects.equals(VIEW_PATH, that.VIEW_PATH) && Objects.equals(VIEW_NAME, that.VIEW_NAME);
    }

    @Override
    public int hashCode() {
      return Objects.hash(VIEW_PATH, VIEW_NAME);
    }

    @Override
    public String toString() {
      return "ShowViewResult{"
          + "VIEW_PATH='"
          + VIEW_PATH
          + '\''
          + ", VIEW_NAME='"
          + VIEW_NAME
          + '\''
          + '}';
    }
  }
}

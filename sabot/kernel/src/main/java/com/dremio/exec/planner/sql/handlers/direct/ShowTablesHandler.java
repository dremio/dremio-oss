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
import com.dremio.exec.planner.sql.parser.SqlShowTables;
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
import java.util.stream.StreamSupport;
import org.apache.calcite.sql.SqlNode;

/**
 * Handler for show tables.
 *
 * <p>SHOW TABLES [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue ] [ ( FROM | IN ) source ] [
 * LIKE 'pattern' ]
 */
public class ShowTablesHandler implements SqlDirectHandler<ShowTablesHandler.ShowTableResult> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ShowTablesHandler.class);

  private final Catalog catalog;
  private final OptionResolver optionResolver;
  private final UserSession userSession;

  public ShowTablesHandler(
      Catalog catalog, OptionResolver optionResolver, UserSession userSession) {
    this.catalog = catalog;
    this.optionResolver = optionResolver;
    this.userSession = userSession;
  }

  @Override
  public List<ShowTableResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlShowTables sqlShowTables = SqlNodeUtil.unwrap(sqlNode, SqlShowTables.class);

    final NamespaceKey sourcePath =
        VersionedHandlerUtils.resolveSourceNameAsNamespaceKey(
            sqlShowTables.getSourceName(), userSession.getDefaultSchemaPath());

    final String sourceName = sourcePath.getRoot();
    VersionContext statementSourceVersion =
        ReferenceTypeUtils.map(
            sqlShowTables.getRefType(), sqlShowTables.getRefValue(), sqlShowTables.getTimestamp());
    final VersionContext sessionVersion = userSession.getSessionVersionForSource(sourceName);
    final VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);

    CatalogEntityKey sourceKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(sourcePath.getPathComponents())
            .tableVersionContext(TableVersionContext.of(sourceVersion))
            .build();

    validate(sourceKey, catalog);

    if (CatalogUtil.requestedPluginSupportsVersionedTables(sourcePath, catalog)) {
      return showVersionedTables(sqlNode, sourceKey);
    } else {
      return showTables(sqlNode, sourcePath);
    }
  }

  private List<ShowTableResult> showTables(SqlNode sqlNode, NamespaceKey sourcePath)
      throws ForemanSetupException {
    final SqlShowTables sqlShowTables = SqlNodeUtil.unwrap(sqlNode, SqlShowTables.class);

    validateNonVersionedSql(sqlShowTables, sourcePath);

    final Pattern likePattern = SqlNodeUtil.getPattern(sqlShowTables.getLikePattern());
    final Matcher m = likePattern.matcher("");

    return StreamSupport.stream(catalog.listDatasets(sourcePath).spliterator(), false)
        .filter(table -> m.reset(table.getTableName()).matches())
        .map(table -> new ShowTableResult(table.getSchemaName(), table.getTableName()))
        .collect(Collectors.toList());
  }

  private List<ShowTableResult> showVersionedTables(SqlNode sqlNode, CatalogEntityKey sourceKey)
      throws ForemanSetupException {
    checkVersionedFeatureEnabled(optionResolver, "SHOW TABLES syntax is not supported.");

    final SqlShowTables showTable =
        requireNonNull(SqlNodeUtil.unwrap(sqlNode, SqlShowTables.class));

    final Pattern likePattern = SqlNodeUtil.getPattern(showTable.getLikePattern());
    final Matcher m = likePattern.matcher("");
    final VersionedPlugin versionedPlugin = getVersionedPlugin(sourceKey.getRootEntity(), catalog);
    final List<String> path = sourceKey.getPathWithoutRoot();
    try {
      return versionedPlugin
          .listTablesIncludeNested(path, sourceKey.getTableVersionContext().asVersionContext())
          .filter(table -> m.reset(table.getName()).matches())
          .map(
              entry ->
                  new ShowTableResult(
                      concatSourceNameAndNamespace(sourceKey.getRootEntity(), entry.getNamespace()),
                      entry.getName()))
          .collect(Collectors.toList());
    } catch (ReferenceConflictException e) {
      throw UserException.validationError(e)
          .message(
              "%s has conflict on source %s.",
              sourceKey.getTableVersionContext().asVersionContext(), sourceKey.getRootEntity())
          .buildSilently();
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
          .message(
              "%s not found on source %s.",
              sourceKey.getTableVersionContext().asVersionContext(), sourceKey.getRootEntity())
          .buildSilently();
    }
  }

  @Override
  public Class<ShowTableResult> getResultType() {
    return ShowTableResult.class;
  }

  private void validateNonVersionedSql(SqlShowTables sqlShowTables, NamespaceKey sourcePath) {
    if (sqlShowTables.getRefValue() != null || sqlShowTables.getRefType() != null) {
      throw UserException.unsupportedError()
          .message("Source %s does not support references.", sourcePath.getRoot())
          .build(logger);
    }
  }

  public static class ShowTableResult {
    public final String TABLE_SCHEMA;
    public final String TABLE_NAME;

    public ShowTableResult(String tABLE_SCHEMA, String tABLE_NAME) {
      super();
      TABLE_SCHEMA = tABLE_SCHEMA;
      TABLE_NAME = tABLE_NAME;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ShowTableResult that = (ShowTableResult) o;
      return Objects.equals(TABLE_SCHEMA, that.TABLE_SCHEMA)
          && Objects.equals(TABLE_NAME, that.TABLE_NAME);
    }

    @Override
    public int hashCode() {
      return Objects.hash(TABLE_SCHEMA, TABLE_NAME);
    }

    @Override
    public String toString() {
      return "ShowTableResult{"
          + "TABLE_SCHEMA='"
          + TABLE_SCHEMA
          + '\''
          + ", TABLE_NAME='"
          + TABLE_NAME
          + '\''
          + '}';
    }
  }
}

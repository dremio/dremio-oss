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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.parser.SqlAlterTableSetOption;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

/** Alters table's properties specified using {@link SqlAlterTableSetOption} */
public class AlterTableSetOptionHandler extends SimpleDirectHandler {

  private final Catalog catalog;
  private final UserSession userSession;

  public AlterTableSetOptionHandler(Catalog catalog, UserSession userSession) {
    super();
    this.catalog = catalog;
    this.userSession = userSession;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlAlterTableSetOption sqlTableOption =
        SqlNodeUtil.unwrap(sqlNode, SqlAlterTableSetOption.class);
    final String optionName = sqlTableOption.getName().toString().toLowerCase();

    NamespaceKey path = catalog.resolveSingle(sqlTableOption.getTable());
    final SqlNode value = sqlTableOption.getValue();
    if (value != null && !(value instanceof SqlLiteral) && !(value instanceof SqlIdentifier)) {
      throw SqlExceptionHelper.parseError(
              "SET requires a literal value or identifier to be provided",
              sql,
              value.getParserPosition())
          .buildSilently();
    }

    final String scope = sqlTableOption.getScope();
    if (!"TABLE".equalsIgnoreCase(scope)) {
      throw UserException.validationError().message("[%s] is not supported", sql).buildSilently();
    }

    VersionContext statementSourceVersion =
        sqlTableOption
            .getSqlTableVersionSpec()
            .getTableVersionSpec()
            .getTableVersionContext()
            .asVersionContext();
    final VersionContext sessionVersion = userSession.getSessionVersionForSource(path.getRoot());
    VersionContext sourceVersion = statementSourceVersion.orElse(sessionVersion);
    final ResolvedVersionContext resolvedVersionContext =
        CatalogUtil.resolveVersionContext(catalog, path.getRoot(), sourceVersion);
    final CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(path.getPathComponents())
            .tableVersionContext(TableVersionContext.of(sourceVersion))
            .build();
    final DremioTable table = catalog.getTableNoResolve(catalogEntityKey);
    if (table == null) {
      throw UserException.validationError()
          .message("Table [%s] does not exist", path)
          .buildSilently();
    }

    final ImmutableMap.Builder<String, AttributeValue> tableOptionsMapBuilder =
        new ImmutableMap.Builder<>();
    if (value != null) { // SET option
      final AttributeValue optionValue;
      if (value instanceof SqlIdentifier) {
        optionValue = createIdentifierAttributeValue((SqlIdentifier) value);
      } else {
        optionValue = createAttributeValue((SqlLiteral) value);
      }

      tableOptionsMapBuilder.put(optionName, optionValue);
    } else { // RESET option
      throw UserException.validationError()
          .message("RESET is not supported for %s", path)
          .buildSilently();
    }

    boolean changed = catalog.alterDataset(catalogEntityKey, tableOptionsMapBuilder.build());
    String changedMessage = changed ? "updated" : "did not change";
    return Collections.singletonList(
        SimpleCommandResult.successful("Table [%s] options %s", path, changedMessage));
  }

  static AttributeValue createAttributeValue(final SqlLiteral literal) {
    final Object object = literal.getValue();
    final SqlTypeName typeName = literal.getTypeName();
    switch (typeName) {
      case DOUBLE:
      case FLOAT:
      case DECIMAL:
        return AttributeValue.of(((BigDecimal) object).doubleValue());

      case SMALLINT:
      case TINYINT:
      case BIGINT:
      case INTEGER:
        return AttributeValue.of(((BigDecimal) object).longValue());

      case VARBINARY:
      case VARCHAR:
      case CHAR:
        return AttributeValue.of(((NlsString) object).getValue());

      case BOOLEAN:
        return AttributeValue.of((Boolean) object);

      default:
        throw UserException.validationError()
            .message(
                "Dremio doesn't support assigning literals of type %s in SET statements.", typeName)
            .buildSilently();
    }
  }

  static AttributeValue createIdentifierAttributeValue(SqlIdentifier identifier) {
    return AttributeValue.of(identifier.names);
  }

  public Catalog getCatalog() {
    return catalog;
  }

  protected boolean isVersioned(NamespaceKey path) {
    return CatalogUtil.requestedPluginSupportsVersionedTables(path, catalog);
  }
}

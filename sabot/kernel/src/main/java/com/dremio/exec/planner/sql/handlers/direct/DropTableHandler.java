/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.SqlDropTable;
import com.dremio.exec.store.AbstractSchema;
import com.dremio.exec.store.dfs.SchemaMutability.MutationType;
import com.dremio.exec.work.foreman.ForemanSetupException;

// Direct Handler for dropping a table.
public class DropTableHandler extends SimpleDirectHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropTableHandler.class);

  private final SchemaPlus defaultSchema;
  private final boolean systemUser;

  public DropTableHandler(SchemaPlus defaultSchema, boolean systemUser) {
    super();
    this.defaultSchema = defaultSchema;
    this.systemUser = systemUser;
  }

  /**
   * Function resolves the schema and invokes the drop method
   * (while IF EXISTS statement is used function invokes the drop method only if table exists).
   * Raises an exception if the schema is immutable.
   * @param sqlNode - SqlDropTable (SQL parse tree of drop table [if exists] query)
   * @return - Single row indicating drop succeeded or table is not found while IF EXISTS statement is used,
   * raise exception otherwise
   * @throws ValidationException
   * @throws RelConversionException
   * @throws IOException
   */
  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws ValidationException, ForemanSetupException,
  RelConversionException, IOException {

    String tableName;
    SqlDropTable dropTableNode = ((SqlDropTable) sqlNode);
    SqlIdentifier tableIdentifier = dropTableNode.getTableIdentifier();
    AbstractSchema schemaInstance = null;

    if (tableIdentifier != null) {
      schemaInstance = SchemaUtilities.resolveToMutableSchemaInstance(defaultSchema, dropTableNode.getSchema(), systemUser, MutationType.TABLE);
    }

    tableName = dropTableNode.getName();
    if (schemaInstance == null) {
      throw UserException.validationError().message("Invalid table_name [%s]", tableName).build(logger);
    }

    if (dropTableNode.checkTableExistence()) {
      final Table tableToDrop = SqlHandlerUtil.getTableFromSchema(schemaInstance, tableName);
      if (tableToDrop == null || tableToDrop.getJdbcTableType() != Schema.TableType.TABLE) {
        return Collections.singletonList(new SimpleCommandResult(true,
            String.format("Table [%s] not found", tableName)));
      }
    }

    schemaInstance.dropTable(tableName);
    return Collections.singletonList(SimpleCommandResult.successful("Table [%s] dropped", tableName));

  }
}

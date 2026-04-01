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
package com.dremio.plugins.clickhouse;

import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds SQL queries for ClickHouse. */
public class ClickHouseQueryBuilder {

  private static final Logger logger = LoggerFactory.getLogger(ClickHouseQueryBuilder.class);

  private final String database;

  public ClickHouseQueryBuilder(String database) {
    this.database = database;
  }

  /** Build SELECT query for a table. */
  public String buildSelectQuery(
      String tableName, List<String> columns, String whereClause, Long limit, Long offset) {
    StringBuilder sb = new StringBuilder("SELECT ");

    if (columns == null || columns.isEmpty()) {
      sb.append("*");
    } else {
      sb.append(columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", ")));
    }

    sb.append(" FROM ")
        .append(quoteIdentifier(database))
        .append(".")
        .append(quoteIdentifier(tableName));

    if (whereClause != null && !whereClause.isEmpty()) {
      sb.append(" WHERE ").append(whereClause);
    }

    if (limit != null && limit > 0) {
      sb.append(" LIMIT ").append(limit);
    }

    if (offset != null && offset > 0) {
      sb.append(" OFFSET ").append(offset);
    }

    return sb.toString();
  }

  /** Build COUNT query. */
  public String buildCountQuery(String tableName, String whereClause) {
    StringBuilder sb = new StringBuilder("SELECT count(*) FROM ");
    sb.append(quoteIdentifier(database)).append(".").append(quoteIdentifier(tableName));

    if (whereClause != null && !whereClause.isEmpty()) {
      sb.append(" WHERE ").append(whereClause);
    }

    return sb.toString();
  }

  /** Build SHOW TABLES query. */
  public String buildShowTablesQuery() {
    return "SHOW TABLES";
  }

  /** Build DESCRIBE TABLE query. */
  public String buildDescribeTableQuery(String tableName) {
    return "DESCRIBE TABLE " + quoteIdentifier(database) + "." + quoteIdentifier(tableName);
  }

  /** Build EXPLAIN query. */
  public String buildExplainQuery(String query) {
    return "EXPLAIN " + query;
  }

  /** Quote identifier to prevent SQL injection. */
  private String quoteIdentifier(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return identifier;
    }
    // ClickHouse uses backticks for identifiers
    return "`" + identifier.replace("`", "``") + "`";
  }

  /** Convert Dremio type to ClickHouse type. */
  public static String convertType(String dremioType) {
    switch (dremioType.toUpperCase()) {
      case "INTEGER":
      case "INT":
        return "Int32";
      case "BIGINT":
        return "Int64";
      case "SMALLINT":
        return "Int16";
      case "TINYINT":
        return "Int8";
      case "FLOAT":
      case "REAL":
        return "Float32";
      case "DOUBLE":
        return "Float64";
      case "VARCHAR":
      case "CHAR":
      case "STRING":
        return "String";
      case "BOOLEAN":
        return "UInt8";
      case "DATE":
        return "Date";
      case "TIMESTAMP":
        return "DateTime64";
      case "DECIMAL":
        return "Decimal";
      default:
        return "String";
    }
  }
}

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
package com.dremio.jdbc.impl;

import static com.dremio.jdbc.impl.DremioMetaImpl.DECIMAL_DIGITS_DOUBLE;
import static com.dremio.jdbc.impl.DremioMetaImpl.DECIMAL_DIGITS_FLOAT;
import static com.dremio.jdbc.impl.DremioMetaImpl.DECIMAL_DIGITS_REAL;
import static com.dremio.jdbc.impl.DremioMetaImpl.RADIX_DATETIME;
import static com.dremio.jdbc.impl.DremioMetaImpl.RADIX_INTERVAL;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData.StructType;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Pat;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MetaImpl.MetaColumn;
import org.apache.calcite.avatica.MetaImpl.MetaSchema;
import org.apache.calcite.avatica.MetaImpl.MetaTable;

import com.dremio.common.util.concurrent.DremioFutures;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserProtos.CatalogMetadata;
import com.dremio.exec.proto.UserProtos.ColumnMetadata;
import com.dremio.exec.proto.UserProtos.GetCatalogsResp;
import com.dremio.exec.proto.UserProtos.GetColumnsResp;
import com.dremio.exec.proto.UserProtos.GetSchemasResp;
import com.dremio.exec.proto.UserProtos.GetTablesResp;
import com.dremio.exec.proto.UserProtos.LikeFilter;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.SchemaMetadata;
import com.dremio.exec.proto.UserProtos.TableMetadata;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcFuture;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * A server implementation of {@code DremioMeta} using Dremio server API
 */
class DremioMetaServerImpl implements DremioMeta {
  private final DremioConnectionImpl connection;
  private final String searchStringEscape;

  public DremioMetaServerImpl(DremioConnectionImpl connection) throws SQLException {
    this.connection = connection;
    this.searchStringEscape = connection.getMetaData().getSearchStringEscape();
  }

  private LikeFilter newLikeFilter(final Pat pattern) throws SQLException {
    if (pattern == null || pattern.s == null) {
      return null;
    }

    // Escape character is same as AvaticaDatabaseMetaData#getSearchStringEscape()
    return LikeFilter.newBuilder().setPattern(pattern.s).setEscape(searchStringEscape).build();
  }

  /**
   * Quote the provided string as a LIKE pattern
   *
   * @param v the value to quote
   * @return a LIKE pattern matching exactly v, or {@code null} if v is {@code null}
   */
  private Pat quote(String v) throws SQLException {
    if (v == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder(v.length());
    for(int index = 0; index<v.length(); index++) {
      char c = v.charAt(index);
      switch(c) {
      case '%':
      case '_':
        sb.append(searchStringEscape).append(c);
        break;

      default:
        if (c == searchStringEscape.charAt(0)) {
          sb.append(searchStringEscape);
        }
        sb.append(c);
      }
    }

    return Pat.of(sb.toString());
  }

  private abstract class MetadataAdapter<CalciteMetaType, Response, ResponseValue> {
    private final Class<? extends CalciteMetaType> clazz;

    public MetadataAdapter(Class<? extends CalciteMetaType> clazz) {
      this.clazz = clazz;
    }

    MetaResultSet getMeta(RpcFuture<Response> future) throws SQLException {
      final Response response;
      try {
        response = DremioFutures.getChecked(future, RpcException.class, RpcException::mapException);
      } catch (RpcException e) {
        throw new SQLException("Failure getting metadata", e);
      }

      // Manage errors
      if (getStatus(response) != RequestStatus.OK) {
        DremioPBError error = getError(response);
        throw new SQLException("Failure getting metadata: " + error.getMessage());
      }

      try {
        List<Object> rows = Lists.transform(getResult(response), new Function<ResponseValue, Object>() {
          @Override
          public Object apply(ResponseValue input) {
            return adapt(input);
          }
        });

        Meta.Frame frame = Meta.Frame.create(0, true, rows);
        StructType fieldMetaData = DremioMetaImpl.fieldMetaData(clazz);
        Meta.Signature signature = Meta.Signature.create(
          fieldMetaData.columns, "",
          Collections.emptyList(),
          CursorFactory.record(clazz, Arrays.asList(clazz.getFields()),
            fieldMetaData.columns.stream().map(column -> column.columnName).collect(Collectors.toList())),
          StatementType.SELECT);

        AvaticaStatement statement = connection.createStatement();
        return MetaResultSet.create(connection.id, statement.getId(), true,
            signature, frame);
      } catch (SQLException e) {
        throw e;
      } catch (Exception e) {
        throw new SQLException("Failure while attempting to get DatabaseMetadata.", e);
      }
    }

    protected abstract RequestStatus getStatus(Response response);
    protected abstract DremioPBError getError(Response response);
    protected abstract List<ResponseValue> getResult(Response response);
    protected abstract CalciteMetaType adapt(ResponseValue protoValue);
  }

  @Override
  public MetaResultSet getCatalogs() throws SQLException {
    return new MetadataAdapter<MetaImpl.MetaCatalog, GetCatalogsResp, CatalogMetadata>(MetaImpl.MetaCatalog.class) {
      @Override
      protected RequestStatus getStatus(GetCatalogsResp response) {
        return response.getStatus();
      }

      @Override
      protected List<CatalogMetadata> getResult(GetCatalogsResp response) {
        return response.getCatalogsList();
      }

      @Override
      protected DremioPBError getError(GetCatalogsResp response) {
        return response.getError();
      }

      @Override
      protected MetaImpl.MetaCatalog adapt(CatalogMetadata protoValue) {
        return new MetaImpl.MetaCatalog(protoValue.getCatalogName());
      }
    }.getMeta(connection.getClient().getCatalogs(null));
  }

  @Override
  public MetaResultSet getSchemas(String catalog, Pat schemaPattern) throws SQLException {
    final LikeFilter catalogNameFilter = newLikeFilter(quote(catalog));
    final LikeFilter schemaNameFilter = newLikeFilter(schemaPattern);

    return new MetadataAdapter<MetaImpl.MetaSchema, GetSchemasResp, SchemaMetadata>(MetaImpl.MetaSchema.class) {
      @Override
      protected RequestStatus getStatus(GetSchemasResp response) {
        return response.getStatus();
      }

      @Override
      protected List<SchemaMetadata> getResult(GetSchemasResp response) {
        return response.getSchemasList();
      }

      @Override
      protected DremioPBError getError(GetSchemasResp response) {
        return response.getError();
      }

      @Override
      protected MetaSchema adapt(SchemaMetadata value) {
        return new MetaImpl.MetaSchema(value.getCatalogName(), value.getSchemaName());
      }
    }.getMeta(connection.getClient().getSchemas(catalogNameFilter, schemaNameFilter));
  }

  @Override
  public MetaResultSet getTables(String catalog, final Pat schemaPattern, final Pat tableNamePattern,
      final List<String> typeList) throws SQLException {
    // Catalog is not a pattern
    final LikeFilter catalogNameFilter = newLikeFilter(quote(catalog));
    final LikeFilter schemaNameFilter = newLikeFilter(schemaPattern);
    final LikeFilter tableNameFilter = newLikeFilter(tableNamePattern);

    return new MetadataAdapter<MetaImpl.MetaTable, GetTablesResp, TableMetadata>(MetaTable.class) {

      @Override
      protected RequestStatus getStatus(GetTablesResp response) {
        return response.getStatus();
      };

      @Override
      protected DremioPBError getError(GetTablesResp response) {
        return response.getError();
      };

      @Override
      protected List<TableMetadata> getResult(GetTablesResp response) {
        return response.getTablesList();
      }

      @Override
      protected MetaImpl.MetaTable adapt(TableMetadata protoValue) {
        return new MetaImpl.MetaTable(protoValue.getCatalogName(), protoValue.getSchemaName(), protoValue.getTableName(), protoValue.getType());
      };
    }.getMeta(connection.getClient().getTables(catalogNameFilter, schemaNameFilter, tableNameFilter, typeList));
  }

  @Override
  public MetaResultSet getColumns(String catalog, Pat schemaPattern,
      Pat tableNamePattern, Pat columnNamePattern) throws SQLException{
    final LikeFilter catalogNameFilter = newLikeFilter(quote(catalog));
    final LikeFilter schemaNameFilter = newLikeFilter(schemaPattern);
    final LikeFilter tableNameFilter = newLikeFilter(tableNamePattern);
    final LikeFilter columnNameFilter = newLikeFilter(columnNamePattern);

    return new MetadataAdapter<MetaColumn, GetColumnsResp, ColumnMetadata>(MetaColumn.class) {
      @Override
      protected RequestStatus getStatus(GetColumnsResp response) {
        return response.getStatus();
      }

      @Override
      protected DremioPBError getError(GetColumnsResp response) {
        return response.getError();
      }

      @Override
      protected List<ColumnMetadata> getResult(GetColumnsResp response) {
        return response.getColumnsList();
      };

      private int getDataType(ColumnMetadata value) {
        switch (value.getDataType()) {
        case "ARRAY":
          return Types.ARRAY;

        case "BIGINT":
          return Types.BIGINT;
        case "BINARY":
          return Types.BINARY;
        case "BINARY LARGE OBJECT":
          return Types.BLOB;
        case "BINARY VARYING":
          return Types.VARBINARY;
        case "BIT":
          return Types.BIT;
        case "BOOLEAN":
          return Types.BOOLEAN;
        case "CHARACTER":
          return Types.CHAR;
          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "CHARACTER LARGE OBJECT":
          return Types.CLOB;
        case "CHARACTER VARYING":
          return Types.VARCHAR;

          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "DATALINK":
          return Types.DATALINK;
        case "DATE":
          return Types.DATE;
        case "DECIMAL":
          return Types.DECIMAL;
          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "DISTINCT":
          return Types.DISTINCT;
        case "DOUBLE":
        case "DOUBLE PRECISION":
          return Types.DOUBLE;

        case "FLOAT":
          return Types.FLOAT;

        case "INTEGER":
          return Types.INTEGER;
        case "INTERVAL":
          return Types.OTHER;

          // Resolve: Not seen in Dremio yet. Can it ever appear?:
        case "JAVA_OBJECT":
          return Types.JAVA_OBJECT;

          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "LONGNVARCHAR":
          return Types.LONGNVARCHAR;
          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "LONGVARBINARY":
          return Types.LONGVARBINARY;
          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "LONGVARCHAR":
          return Types.LONGVARCHAR;

        case "MAP":
          return Types.OTHER;

          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "NATIONAL CHARACTER":
          return Types.NCHAR;
          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "NATIONAL CHARACTER LARGE OBJECT":
          return Types.NCLOB;
          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "NATIONAL CHARACTER VARYING":
          return Types.NVARCHAR;

          // TODO: Resolve following about NULL (and then update comment and
          // code):
          // It is not clear whether Types.NULL can represent a type (perhaps the
          // type of the literal NULL when no further type information is known?)
          // or
          // whether 'NULL' can appear in INFORMATION_SCHEMA.COLUMNS.DATA_TYPE.
          // For now, since it shouldn't hurt, include 'NULL'/Types.NULL in
          // mapping.
        case "NULL":
          return Types.NULL;
          // (No NUMERIC--Dremio seems to map any to DECIMAL currently.)
        case "NUMERIC":
          return Types.NUMERIC;

          // Resolve: Unexpectedly, has appeared in Dremio. Should it?
        case "OTHER":
          return Types.OTHER;

        case "REAL":
          return Types.REAL;
          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "REF":
          return Types.REF;
          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "ROWID":
          return Types.ROWID;

        case "SMALLINT":
          return Types.SMALLINT;
          // Resolve: Not seen in Dremio yet. Can it appear?:
        case "SQLXML":
          return Types.SQLXML;
        case "STRUCT":
          return Types.STRUCT;

        case "TIME":
          return Types.TIME;
        case "TIMESTAMP":
          return Types.TIMESTAMP;
        case "TINYINT":
          return Types.TINYINT;

        default:
          return Types.OTHER;
        }
      }

      Integer getDecimalDigits(ColumnMetadata value) {
        switch(value.getDataType()) {
        case "TINYINT":
        case "SMALLINT":
        case "INTEGER":
        case "BIGINT":
        case "DECIMAL":
        case "NUMERIC":
          return value.hasNumericScale() ? value.getNumericScale() : null;

        case "REAL":
          return DECIMAL_DIGITS_REAL;

        case "FLOAT":
          return DECIMAL_DIGITS_FLOAT;

        case "DOUBLE":
          return DECIMAL_DIGITS_DOUBLE;

        case "DATE":
        case "TIME":
        case "TIMESTAMP":
        case "INTERVAL":
          return value.getDateTimePrecision();

        default:
          return null;
        }
      }

      private Integer getNumPrecRadix(ColumnMetadata value) {
        switch(value.getDataType()) {
        case "TINYINT":
        case "SMALLINT":
        case "INTEGER":
        case "BIGINT":
        case "DECIMAL":
        case "NUMERIC":
        case "REAL":
        case "FLOAT":
        case "DOUBLE":
          return value.getNumericPrecisionRadix();

        case "INTERVAL":
          return RADIX_INTERVAL;

        case "DATE":
        case "TIME":
        case "TIMESTAMP":
          return RADIX_DATETIME;

        default:
          return null;
        }
      }

      private int getNullable(ColumnMetadata value) {
        if (!value.hasIsNullable()) {
          return DatabaseMetaData.columnNullableUnknown;
        }
        return  value.getIsNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls;
      }

      private String getIsNullable(ColumnMetadata value) {
        if (!value.hasIsNullable()) {
          return "";
        }
        return  value.getIsNullable() ? "YES" : "NO";
      }

      private Integer getCharOctetLength(ColumnMetadata value) {
        if (!value.hasCharMaxLength()) {
          return null;
        }

        switch(value.getDataType()) {
        case "CHARACTER":
        case "CHARACTER LARGE OBJECT":
        case "CHARACTER VARYING":
        case "LONGVARCHAR":
        case "LONGNVARCHAR":
        case "NATIONAL CHARACTER":
        case "NATIONAL CHARACTER LARGE OBJECT":
        case "NATIONAL CHARACTER VARYING":
          return value.getCharOctetLength();

        default:
          return null;
        }
      }

      @Override
      protected MetaColumn adapt(ColumnMetadata value) {
        return new MetaColumn(
            value.getCatalogName(),
            value.getSchemaName(),
            value.getTableName(),
            value.getColumnName(),
            getDataType(value), // It might require the full SQL type
            value.getDataType(),
            value.getColumnSize(),
            getDecimalDigits(value),
            getNumPrecRadix(value),
            getNullable(value),
            getCharOctetLength(value),
            value.getOrdinalPosition(),
            getIsNullable(value));
      }
    }.getMeta(connection.getClient().getColumns(catalogNameFilter, schemaNameFilter, tableNameFilter, columnNameFilter));
  }
}

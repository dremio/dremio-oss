/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.sql.handlers.commands;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;
import org.joda.time.Period;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos.ColumnSearchability;
import com.dremio.exec.proto.UserProtos.ColumnUpdatability;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.proto.UserProtos.PreparedStatement;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.ResultColumnMetadata;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.work.protector.ResponseSenderHandler;
import com.google.common.collect.ImmutableMap;

/**
 * Contains worker {@link Runnable} for creating a prepared statement and helper methods.
 */
public class PreparedStatementProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PreparedStatementProvider.class);


  /**
   * Static list of mappings from {@link MinorType} to JDBC ResultSet class name (to be returned through
   * {@link ResultSetMetaData#getColumnClassName(int)}.
   */
  private static final Map<MinorType, String> MINOR_TYPE_TO_JDBC_CLASSNAME = ImmutableMap.<MinorType, String>builder()
      .put(MinorType.INT, Integer.class.getName())
      .put(MinorType.BIGINT, Long.class.getName())
      .put(MinorType.FLOAT4, Float.class.getName())
      .put(MinorType.FLOAT8, Double.class.getName())
      .put(MinorType.VARCHAR, String.class.getName())
      .put(MinorType.BIT, Boolean.class.getName())
      .put(MinorType.DATE, Date.class.getName())
      .put(MinorType.DECIMAL, BigDecimal.class.getName())
      .put(MinorType.DECIMAL9, BigDecimal.class.getName())
      .put(MinorType.DECIMAL18, BigDecimal.class.getName())
      .put(MinorType.DECIMAL28SPARSE, BigDecimal.class.getName())
      .put(MinorType.DECIMAL38SPARSE, BigDecimal.class.getName())
      .put(MinorType.TIME, Time.class.getName())
      .put(MinorType.TIMESTAMP, Timestamp.class.getName())
      .put(MinorType.VARBINARY, byte[].class.getName())
      .put(MinorType.INTERVALYEAR, Period.class.getName())
      .put(MinorType.INTERVALDAY, Period.class.getName())
      .put(MinorType.MAP, Object.class.getName())
      .put(MinorType.LIST, Object.class.getName())
      .put(MinorType.UNION, Object.class.getName())
      .build();

  public static CreatePreparedStatementResp build(BatchSchema schema, ServerPreparedStatementState handle,
                                                  QueryId queryId, String catalogName) {
    final CreatePreparedStatementResp.Builder respBuilder = CreatePreparedStatementResp.newBuilder();
    final PreparedStatement.Builder prepStmtBuilder = PreparedStatement.newBuilder();
    prepStmtBuilder.setServerHandle(PreparedStatementHandle.newBuilder().setServerInfo(handle.toByteString()));
    for (Field field : schema) {
      prepStmtBuilder.addColumns(serializeColumn(field, catalogName));
    }
    respBuilder.setStatus(RequestStatus.OK);
    respBuilder.setPreparedStatement(prepStmtBuilder.build());
    respBuilder.setQueryId(queryId);
    return respBuilder.build();
  }

  /**
   * Serialize the given {@link Field} into a {@link ResultColumnMetadata}.
   * @param field
   * @return
   */
  private static ResultColumnMetadata serializeColumn(Field field, String catalogName) {
    final ResultColumnMetadata.Builder builder = ResultColumnMetadata.newBuilder();
    final CompleteType type = CompleteType.fromField(field);
//    final MajorType majorType = field.getMajorType();
//    final MinorType minorType = majorType.getMinorType();

    builder.setCatalogName(catalogName);

    /*
     * Designated column's schema name. Empty string if not applicable. Initial implementation defaults to empty string
     * as we use LIMIT 0 queries to get the schema and schema info is lost. If we derive the schema from plan, we may
     * get the right value.
     */
    builder.setSchemaName("");

    /*
     * Designated column's table name. Not set if not applicable. Initial implementation defaults to empty string as
     * we use LIMIT 0 queries to get the schema and table info is lost. If we derive the table from plan, we may get
     * the right value.
     */
    builder.setTableName("");

    builder.setColumnName(field.getName());

    /*
     * Column label name for display or print purposes.
     * Ex. a column named "empName" might be labeled as "Employee Name".
     * Initial implementation defaults to same value as column name.
     */
    builder.setLabel(field.getName());

    /*
     * Data type in string format. Value is SQL standard type.
     */
    builder.setDataType(type.getSqlTypeName());

    builder.setIsNullable(true);

    /*
     * For numeric data, this is the maximum precision.
     * For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation
     *    (assuming the maximum allowed precision of the fractional seconds component).
     * For binary data, this is the length in bytes.
     * For all other types 0 is returned where the column size is not applicable.
     */
    Integer precision = type.getPrecision();
    if(precision != null){
      builder.setPrecision(precision);
    }

    /*
     * Column's number of digits to right of the decimal point. 0 is returned for types where the scale is not applicable
     */
    Integer scale = type.getScale();
    if(scale != null){
      builder.setScale(scale);
    }

    /*
     * Indicates whether values in the designated column are signed numbers.
     */
    builder.setSigned(type.isSigned());

    /*
     * Maximum number of characters required to display data from the column.
     */
    builder.setDisplaySize(type.getSqlDisplaySize());

    /*
     * Is the column an aliased column. Initial implementation defaults to true as we derive schema from LIMIT 0 query and
     * not plan
     */
    builder.setIsAliased(true);

    builder.setSearchability(ColumnSearchability.ALL);
    builder.setUpdatability(ColumnUpdatability.READ_ONLY);
    builder.setAutoIncrement(false);
    builder.setCaseSensitivity(false);
    builder.setSortable(type.isSortable());

    /*
     * Returns the fully-qualified name of the Java class whose instances are manufactured if the method
     * ResultSet.getObject is called to retrieve a value from the column. Applicable only to JDBC clients.
     */
    builder.setClassName(MINOR_TYPE_TO_JDBC_CLASSNAME.get(type.toMinorType()));

    builder.setIsCurrency(false);

    return builder.build();
  }

  public static class PreparedStatementHandler extends ResponseSenderHandler<CreatePreparedStatementResp> {

    public PreparedStatementHandler(ResponseSender sender) {
      super(RpcType.PREPARED_STATEMENT, CreatePreparedStatementResp.class, sender);
    }

    @Override
    protected CreatePreparedStatementResp getException(UserException ex) {
      final CreatePreparedStatementResp.Builder respBuilder = CreatePreparedStatementResp.newBuilder();
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(ex.getOrCreatePBError(false));
      return respBuilder.build();
    }

  }


}

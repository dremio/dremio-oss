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
package com.dremio.service.flight.impl;

import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;

import com.dremio.exec.proto.UserProtos;
import com.dremio.service.flight.TicketContent;
import com.dremio.service.flight.protector.CancellableUserResponseHandler;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Container resulting from execution of a CREATE_PREPARED_STATEMENT job. Results from the job are
 * consumed, and then Flight objects are exposed.
 */
public class FlightPreparedStatement {

  private final CancellableUserResponseHandler<UserProtos.CreatePreparedStatementArrowResp>
      responseHandler;

  public FlightPreparedStatement(
      CancellableUserResponseHandler<UserProtos.CreatePreparedStatementArrowResp> responseHandler) {
    this.responseHandler = responseHandler;
  }

  /**
   * Returns a FlightInfo for the PreparedStatement which a given instance manages.
   *
   * @param location The server location.
   * @return The FlightInfo.
   */
  public FlightInfo getFlightInfo(Optional<Location> location) {
    final UserProtos.PreparedStatementArrow preparedStatement =
        responseHandler.get().getPreparedStatement();
    final Schema schema = buildSchema(preparedStatement.getArrowSchema());

    final FlightSql.CommandPreparedStatementQuery command =
        FlightSql.CommandPreparedStatementQuery.newBuilder()
            .setPreparedStatementHandle(preparedStatement.toByteString())
            .build();
    final FlightDescriptor flightDescriptor =
        FlightDescriptor.command(Any.pack(command).toByteArray());
    final Ticket ticket = new Ticket(Any.pack(command).toByteArray());

    final FlightEndpoint flightEndpoint =
        location
            .map(value -> new FlightEndpoint(ticket, value))
            .orElseGet(() -> new FlightEndpoint(ticket));
    return new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);
  }

  /**
   * Returns a FlightInfo for the PreparedStatement which a given instance manages. This method is
   * for returning a lightweight ticket for non-Flight-SQL queries.
   *
   * @param location The server location.
   * @return The FlightInfo.
   */
  public FlightInfo getFlightInfoLegacy(
      Optional<Location> location, FlightDescriptor flightDescriptor) {
    final UserProtos.CreatePreparedStatementArrowResp createPreparedStatementResp =
        responseHandler.get();
    final Schema schema =
        buildSchema(createPreparedStatementResp.getPreparedStatement().getArrowSchema());

    final TicketContent.PreparedStatementTicket preparedStatementTicketContent =
        TicketContent.PreparedStatementTicket.newBuilder()
            .setQuery(FlightWorkManager.getQuery(flightDescriptor))
            .setHandle(createPreparedStatementResp.getPreparedStatement().getServerHandle())
            .build();

    final Ticket ticket = new Ticket(preparedStatementTicketContent.toByteArray());

    final FlightEndpoint flightEndpoint =
        location
            .map(value -> new FlightEndpoint(ticket, value))
            .orElseGet(() -> new FlightEndpoint(ticket));
    return new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);
  }

  /**
   * Create an action to create a prepared statement.
   *
   * @return a ActionCreatePreparedStatementResult;
   */
  public ActionCreatePreparedStatementResult createAction() {
    final UserProtos.PreparedStatementArrow preparedStatement =
        responseHandler.get().getPreparedStatement();
    final Schema schema = buildSchema(preparedStatement.getArrowSchema());
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
    } catch (IOException e) {
      throw new RuntimeException("IO Error when serializing schema '" + schema + "'.", e);
    }

    return ActionCreatePreparedStatementResult.newBuilder()
        .setDatasetSchema(ByteString.copyFrom(ByteBuffer.wrap(outputStream.toByteArray())))
        .setParameterSchema(ByteString.EMPTY)
        .setPreparedStatementHandle(preparedStatement.toByteString())
        .build();
  }

  /**
   * Returns the schema.
   *
   * @return The Schema.
   */
  public Schema getSchema() {
    final UserProtos.PreparedStatementArrow preparedStatement =
        responseHandler.get().getPreparedStatement();
    return buildSchema(preparedStatement.getArrowSchema());
  }

  public UserProtos.PreparedStatementHandle getServerHandle() {
    return responseHandler.get().getPreparedStatement().getServerHandle();
  }

  public static Schema buildSchema(final ByteString arrowSchema) {
    final Schema tempSchema = Schema.deserialize(arrowSchema.asReadOnlyByteBuffer());
    final List<Field> newFieldList = new ArrayList<>();

    for (final Field field : tempSchema.getFields()) {
      final Map<String, String> flightSqlColumnMetadata =
          createFlightSqlColumnMetadata(field.getMetadata());

      newFieldList.add(
          new Field(
              field.getName(),
              new FieldType(
                  field.isNullable(),
                  field.getType(),
                  field.getDictionary(),
                  flightSqlColumnMetadata),
              field.getChildren()));
    }

    return new Schema(newFieldList);
  }

  private static Map<String, String> createFlightSqlColumnMetadata(
      final Map<String, String> column) {
    // Directly influenced by PreparedStatementProvider#createTempFieldMetadata
    final FlightSqlColumnMetadata.Builder flightSqlColumnMetadata =
        new FlightSqlColumnMetadata.Builder();

    final String typeName = column.get("TYPE_NAME");
    if (typeName != null) {
      flightSqlColumnMetadata.typeName(typeName);
    }

    final String dbSchemaName = column.get("SCHEMA_NAME");
    if (dbSchemaName != null) {
      flightSqlColumnMetadata.schemaName(dbSchemaName);
    }

    final String tableName = column.get("TABLE_NAME");
    if (tableName != null) {
      flightSqlColumnMetadata.tableName(tableName);
    }

    final String isAutoIncrement = column.get("IS_AUTO_INCREMENT");
    if (isAutoIncrement != null) {
      flightSqlColumnMetadata.isAutoIncrement(Boolean.parseBoolean(isAutoIncrement));
    }

    final String isCaseSensitive = column.get("IS_CASE_SENSITIVE");
    if (isCaseSensitive != null) {
      flightSqlColumnMetadata.isCaseSensitive(Boolean.parseBoolean(isCaseSensitive));
    }

    final String isReadOnly = column.get("IS_READ_ONLY");
    if (isReadOnly != null) {
      flightSqlColumnMetadata.isReadOnly(Boolean.parseBoolean(isReadOnly));
    }

    final String isSearchable = column.get("IS_SEARCHABLE");
    if (isSearchable != null) {
      flightSqlColumnMetadata.isSearchable(Boolean.parseBoolean(isSearchable));
    }

    final String precision = column.get("PRECISION");
    if (precision != null) {
      flightSqlColumnMetadata.precision(Integer.parseInt(precision));
    }

    final String scale = column.get("SCALE");
    if (scale != null) {
      flightSqlColumnMetadata.scale(Integer.parseInt(scale));
    }

    return flightSqlColumnMetadata.build().getMetadataMap();
  }
}

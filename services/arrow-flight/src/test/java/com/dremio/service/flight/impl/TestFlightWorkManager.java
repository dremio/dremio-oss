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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import com.dremio.exec.proto.UserProtos;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for FlightWorkManager.
 */
public class TestFlightWorkManager {

  @Test
  public void testGetQueryPath() {
    final FlightDescriptor flightDescriptor = FlightDescriptor.path("pathelement");

    // Act
    assertThatThrownBy(() -> FlightWorkManager.getQuery(flightDescriptor))
      .isInstanceOf(FlightRuntimeException.class)
      .hasMessageContaining("FlightDescriptor type Path is unimplemented.");
  }

  @Test
  public void testGetQueryNullCommand() {
    final FlightDescriptor flightDescriptor = FlightDescriptor.command(null);

    // Act
    assertThatThrownBy(() -> FlightWorkManager.getQuery(flightDescriptor))
      .isInstanceOf(FlightRuntimeException.class)
      .hasMessageContaining("FlightDescriptor type Cmd must have content in the cmd member.");
  }

  @Test
  public void testGetQueryEmptyCommand() {
    // Arrange
    final FlightDescriptor flightDescriptor = FlightDescriptor.command(new byte[0]);

    // Act
    String actual = FlightWorkManager.getQuery(flightDescriptor);

    // Assert
    assertEquals("", actual);
  }

  @Test
  public void testGetQueryCorrectCommand() {
    // Arrange
    final String expected = "command";
    final FlightDescriptor flightDescriptor = FlightDescriptor.command(expected.getBytes(StandardCharsets.UTF_8));

    // Act
    final String actual = FlightWorkManager.getQuery(flightDescriptor);

    // Assert
    assertEquals(expected, actual);
  }

  @Test
  public void testGetTablesEmptyFields() {
    // Arrange
    final byte[] expected = FlightWorkManager.getSerializedSchema(Collections.emptyList());

    // Act
    final byte[] actual = FlightWorkManager.getSerializedSchema(null);

    // Assert
    assertArrayEquals(expected, actual);
  }

  @Test
  public void testGetTablesNonEmptyFields() throws IOException {
    // Arrange
    final List<Field> expectedClean = new ArrayList<Field>() {
      {
        add(new Field("col1", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
        add(new Field("col2", FieldType.nullable(new ArrowType.Int(16, true)), null));
        add(new Field("col3", FieldType.nullable(ArrowType.Binary.INSTANCE), null));
      }
    };
    final byte[] expectedSerialized = FlightWorkManager.getSerializedSchema(expectedClean);

    // Act
    final Schema actualSchema = MessageSerializer.deserializeSchema(
      new ReadChannel(Channels.newChannel(
        new ByteArrayInputStream(expectedSerialized))));

    // Assert
    assertEquals(expectedClean, actualSchema.getFields());
  }

  /**
   * Tests that the arrow fields to table mapping does not mix up columns from tables that have
   * the same name but in different namespaces.
   */
  @Test
  public void testBuildArrowFieldsByTableMap() {
    final UserProtos.TableMetadata tableMetadata1 = UserProtos.TableMetadata.newBuilder()
      .setSchemaName("schema1")
      .setTableName("table")
      .build();

    final UserProtos.TableMetadata tableMetadata2 = UserProtos.TableMetadata.newBuilder()
      .setSchemaName("schema2")
      .setTableName("table")
      .build();

    final Field column1 = new Field("column1",
      new FieldType(false,
        new ArrowType.Int(32, true),
        null,
        new FlightSqlColumnMetadata.Builder()
          .schemaName("schema1")
          .tableName("table")
          .typeName("INTEGER")
          .isAutoIncrement(false)
          .isCaseSensitive(false)
          .isReadOnly(true)
          .isSearchable(true)
          .build().getMetadataMap()),
      Collections.emptyList());

    final Field column2 = new Field("column2",
      new FieldType(false, new ArrowType.Utf8(),
        null,
        new FlightSqlColumnMetadata.Builder()
          .schemaName("schema2")
          .tableName("table")
          .typeName("CHARACTER VARYING")
          .isAutoIncrement(false)
          .isCaseSensitive(false)
          .isReadOnly(true)
          .isSearchable(true)
          .precision(65536)
          .build().getMetadataMap()),
      Collections.emptyList());

    final ImmutableMap<UserProtos.TableMetadata, List<Field>> expectedMap = ImmutableMap.of(
      tableMetadata1, Collections.singletonList(column1),
      tableMetadata2, Collections.singletonList(column2));

    UserProtos.GetColumnsResp columnsResp = UserProtos.GetColumnsResp.newBuilder()
      .addColumns(UserProtos.ColumnMetadata.newBuilder()
          .setSchemaName("schema1")
          .setTableName("table")
          .setColumnName("column1")
          .setDataType("INTEGER")
          .build())
      .addColumns(UserProtos.ColumnMetadata.newBuilder()
          .setSchemaName("schema2")
          .setTableName("table")
          .setColumnName("column2")
          .setDataType("CHARACTER VARYING")
          .build())
        .build();
    ImmutableMap<UserProtos.TableMetadata, List<Field>> actualMap = ImmutableMap.copyOf(FlightWorkManager.buildArrowFieldsByTableMap(columnsResp));

    assertEquals(expectedMap, actualMap);
  }
}

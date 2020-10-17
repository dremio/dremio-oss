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

package com.dremio.exec.store.iceberg;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.test.DremioTest;

public class TestSchemaConverter extends DremioTest {
  private SchemaConverter schemaConverter;

  @Before
  public void setup() throws Exception {
    schemaConverter = new SchemaConverter();
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void primitiveBasic() {
    org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
      NestedField.optional(1, "boolean", Types.BooleanType.get()),
      NestedField.optional(2, "int", Types.IntegerType.get()),
      NestedField.optional(3, "long", Types.LongType.get()),
      NestedField.optional(4, "float", Types.FloatType.get()),
      NestedField.optional(5, "double", Types.DoubleType.get()),
      NestedField.optional(6, "decimal_38_16", Types.DecimalType.of(38, 16)),
      NestedField.optional(7, "string", Types.StringType.get()),
      NestedField.optional(8, "binary", Types.BinaryType.get()),
      NestedField.optional(9, "date", Types.DateType.get()),
      NestedField.optional(10, "time", Types.TimeType.get()),
      NestedField.optional(11, "fixed_32", Types.FixedType.ofLength(32)),
      NestedField.optional(12, "timestamp", Types.TimestampType.withZone())
    );

    BatchSchema schema = BatchSchema.newBuilder()
      .addField(CompleteType.BIT.toField("boolean"))
      .addField(CompleteType.INT.toField("int"))
      .addField(CompleteType.BIGINT.toField("long"))
      .addField(CompleteType.FLOAT.toField("float"))
      .addField(CompleteType.DOUBLE.toField("double"))
      .addField(new CompleteType(new Decimal(38, 16)).toField("decimal_38_16"))
      .addField(CompleteType.VARCHAR.toField("string"))
      .addField(CompleteType.VARBINARY.toField("binary"))
      .addField(CompleteType.DATE.toField("date"))
      .addField(CompleteType.TIME.toField("time"))
      .addField(new CompleteType(new ArrowType.FixedSizeBinary(32)).toField("fixed_32"))
      .addField(CompleteType.TIMESTAMP.toField("timestamp"))
      .build();

    assertEquals(schema, schemaConverter.fromIceberg(icebergSchema));
    assertEquals(icebergSchema.toString(), schemaConverter.toIceberg(schema).toString());
  }

  @Test
  public void primitiveTwoWay() {
    BatchSchema schema = BatchSchema.newBuilder()
      .addField(CompleteType.BIT.toField("boolean"))
      .addField(CompleteType.INT.toField("int"))
      .addField(CompleteType.BIGINT.toField("long"))
      .addField(CompleteType.FLOAT.toField("float"))
      .addField(CompleteType.DOUBLE.toField("double"))
      .addField(CompleteType.DATE.toField("date"))
      .addField(CompleteType.TIMESTAMP.toField("time"))
      .addField(CompleteType.VARCHAR.toField("string"))
      .addField(CompleteType.VARBINARY.toField("binary"))
      .addField(new CompleteType(new Decimal(38, 16)).toField("decimal_38_16"))
      .build();

    org.apache.iceberg.Schema icebergSchema = schemaConverter.toIceberg(schema);
    BatchSchema result = schemaConverter.fromIceberg(icebergSchema);
    assertEquals(result, schema);
  }

  @Test
  public void missingArrowTypes() {
    org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
      NestedField.optional(1, "uuid", Types.UUIDType.get())
    );

    BatchSchema schema = BatchSchema.newBuilder()
      .addField(new CompleteType(new FixedSizeBinary(16)).toField("uuid"))
      .build();

    BatchSchema result = schemaConverter.fromIceberg(icebergSchema);
    assertEquals(result, schema);
  }

  @Test
  public void list() {
    BatchSchema schema = BatchSchema.newBuilder()
      .addField(CompleteType.INT.asList().toField("list_int"))
      .addField(CompleteType.INT.asList().asList().toField("list_list_int"))
      .build();

    org.apache.iceberg.Schema icebergSchema = schemaConverter.toIceberg(schema);
    BatchSchema result = schemaConverter.fromIceberg(icebergSchema);
    assertEquals(result, schema);
  }

  @Test
  public void struct() {
    Field struct = CompleteType.struct(
      CompleteType.VARCHAR.toField("varchar"),
      CompleteType.INT.toField("int"),
      CompleteType.BIT.asList().toField("bits")
    ).toField("struct");

    BatchSchema schema = BatchSchema.newBuilder()
      .addField(struct)
      .build();

    org.apache.iceberg.Schema icebergSchema = schemaConverter.toIceberg(schema);
    BatchSchema result = schemaConverter.fromIceberg(icebergSchema);
    assertEquals(result, schema);
  }

  @Test
  public void map() {
    org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
      NestedField.optional(1, "map",
        Types.MapType.ofOptional(2, 3, Types.IntegerType.get(), Types.FloatType.get()))
    );

    List<Field> children = Arrays.asList(
      CompleteType.INT.toField("key"),
      CompleteType.FLOAT.toField("value")
    );
    BatchSchema schema = BatchSchema.newBuilder()
      .addField(new CompleteType(new ArrowType.Map(false), children).toField("map"))
      .build();

    BatchSchema result = schemaConverter.fromIceberg(icebergSchema);
    // dremio silently drops map type columns
    assertEquals(result.getFieldCount(), 0);

    org.apache.iceberg.Schema icebergResult = schemaConverter.toIceberg(schema);
    assertEquals(icebergSchema.toString(), icebergResult.toString());
  }

  @Test
  public void mixed() throws Exception {
    BatchSchema schema = BatchSchema.newBuilder()
      .addField(CompleteType.INT.toField("rownum"))
      .addField(CompleteType.VARCHAR.toField("name"))
      .addField(CompleteType.INT.toField("age"))
      .addField(CompleteType.FLOAT.toField("gpa"))
      .addField(CompleteType.BIGINT.toField("studentnum"))
      .addField(CompleteType.TIMESTAMP.toField("create_time"))
      .addField(CompleteType.VARCHAR.asList().toField("interests"))
      .addField(CompleteType.struct(
        CompleteType.VARCHAR.toField("color"),
        CompleteType.VARCHAR.toField("sport"),
        CompleteType.VARCHAR.toField("food")
      ).toField("favorites"))
      .build();

    org.apache.iceberg.Schema expectedSchema = new org.apache.iceberg.Schema(
      NestedField.optional(1, "rownum", Types.IntegerType.get()),
      NestedField.optional(2, "name", Types.StringType.get()),
      NestedField.optional(3, "age", Types.IntegerType.get()),
      NestedField.optional(4, "gpa", Types.FloatType.get()),
      NestedField.optional(5, "studentnum", Types.LongType.get()),
      NestedField.optional(6, "create_time", Types.TimestampType.withZone()),
      NestedField.optional(7, "interests",
        Types.ListType.ofOptional(9, Types.StringType.get())),
      NestedField.optional(8, "favorites",
        Types.StructType.of(
          NestedField.optional(10, "color", Types.StringType.get()),
          NestedField.optional(11, "sport", Types.StringType.get()),
          NestedField.optional(12, "food", Types.StringType.get())
        ))
    );

    org.apache.iceberg.Schema icebergResult = schemaConverter.toIceberg(schema);
    assertEquals(expectedSchema.toString(), icebergResult.toString());

    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    String rootPath = folder.getRoot().toString();
    Configuration conf = new Configuration();
    IcebergCatalog catalog = new IcebergCatalog(rootPath, conf);
    catalog.beginCreateTable("testTableName", schema, Collections.emptyList());
    catalog.endCreateTable();

    Table table = new HadoopTables(conf).load(rootPath);
    assertEquals(expectedSchema.toString(), table.schema().toString());
  }

  @Test
  public void testPartitionComparatorField() {
    BatchSchema inputschema = BatchSchema.newBuilder()
      .addField(CompleteType.BIT.toField("boolean"))
      .addField(CompleteType.INT.toField("int"))
      .addField(CompleteType.BIT.toField(WriterPrel.PARTITION_COMPARATOR_FIELD))
      .build();

    org.apache.iceberg.Schema expectedSchema = new org.apache.iceberg.Schema(
      NestedField.optional(1, "boolean", Types.BooleanType.get()),
      NestedField.optional(2, "int", Types.IntegerType.get()));

    SchemaConverter convert = new SchemaConverter();
    assertEquals(convert.toIceberg(inputschema).toString(), expectedSchema.toString());
  }

  @Test
  public void unsupportedArrowTypes() {
    BatchSchema inputSchema = BatchSchema.newBuilder()
      .addField(CompleteType.union(
        CompleteType.INT.toField("int_field"),
        CompleteType.BIGINT.toField("bigint_field")
      ).toField("union_field"))
      .build();

    expectedEx.expect(UserException.class);
    expectedEx.expectMessage("conversion from arrow type to iceberg type failed for field union_field");
    SchemaConverter convert = new SchemaConverter();
    convert.toIceberg(inputSchema);
  }

  @Test
  public void unsupportedIcebergTypes() {
    org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
      NestedField.optional(1, "timestamp_nozone_field", Types.TimestampType.withoutZone())
    );

    expectedEx.expect(UserException.class);
    expectedEx.expectMessage("conversion from iceberg type to arrow type failed for field timestamp_nozone_field");
    SchemaConverter convert = new SchemaConverter();
    convert.fromIceberg(schema);
  }
}

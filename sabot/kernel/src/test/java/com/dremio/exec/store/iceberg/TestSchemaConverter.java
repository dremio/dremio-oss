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

import static com.dremio.common.expression.CompleteType.BIGINT;
import static com.dremio.common.expression.CompleteType.BIT;
import static com.dremio.common.expression.CompleteType.DATE;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.TIME;
import static com.dremio.common.expression.CompleteType.TIMESTAMP;
import static com.dremio.common.expression.CompleteType.VARBINARY;
import static com.dremio.common.expression.CompleteType.VARCHAR;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.map.CaseInsensitiveImmutableBiMap;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.FieldIdBroker.SeededFieldIdBroker;
import com.dremio.exec.store.iceberg.FieldIdBroker.UnboundedFieldIdBroker;
import com.dremio.exec.store.iceberg.hadoop.IcebergHadoopModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.test.DremioTest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSchemaConverter extends DremioTest {

  private final SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();

  @Test
  public void primitiveBasic() {
    Schema icebergSchema =
        new Schema(
            optional(1, "boolean", BooleanType.get()),
            optional(2, "int", IntegerType.get()),
            optional(3, "long", LongType.get()),
            optional(4, "float", FloatType.get()),
            optional(5, "double", DoubleType.get()),
            optional(6, "decimal_38_16", DecimalType.of(38, 16)),
            optional(7, "string", StringType.get()),
            optional(8, "binary", BinaryType.get()),
            optional(9, "date", DateType.get()),
            optional(10, "time", TimeType.get()),
            optional(11, "fixed_32", FixedType.ofLength(32)),
            optional(12, "timestamp", TimestampType.withZone()));

    BatchSchema schema =
        BatchSchema.newBuilder()
            .addField(BIT.toField("boolean"))
            .addField(INT.toField("int"))
            .addField(BIGINT.toField("long"))
            .addField(FLOAT.toField("float"))
            .addField(DOUBLE.toField("double"))
            .addField(new CompleteType(new Decimal(38, 16, 128)).toField("decimal_38_16"))
            .addField(VARCHAR.toField("string"))
            .addField(VARBINARY.toField("binary"))
            .addField(DATE.toField("date"))
            .addField(TIME.toField("time"))
            .addField(new CompleteType(new ArrowType.FixedSizeBinary(32)).toField("fixed_32"))
            .addField(TIMESTAMP.toField("timestamp"))
            .build();

    assertEquals(schema, schemaConverter.fromIceberg(icebergSchema));
    assertEquals(icebergSchema.toString(), schemaConverter.toIcebergSchema(schema).toString());
  }

  @Test
  public void primitiveTwoWay() {
    BatchSchema schema =
        BatchSchema.newBuilder()
            .addField(BIT.toField("boolean"))
            .addField(INT.toField("int"))
            .addField(BIGINT.toField("long"))
            .addField(FLOAT.toField("float"))
            .addField(DOUBLE.toField("double"))
            .addField(DATE.toField("date"))
            .addField(TIMESTAMP.toField("time"))
            .addField(VARCHAR.toField("string"))
            .addField(VARBINARY.toField("binary"))
            .addField(new CompleteType(new Decimal(38, 16, 128)).toField("decimal_38_16"))
            .build();

    Schema icebergSchema = schemaConverter.toIcebergSchema(schema);
    BatchSchema result = schemaConverter.fromIceberg(icebergSchema);
    assertEquals(result, schema);
  }

  @Test
  public void missingArrowTypes() {
    Schema icebergSchema = new Schema(optional(1, "uuid", Types.UUIDType.get()));

    BatchSchema schema =
        BatchSchema.newBuilder()
            .addField(new CompleteType(new FixedSizeBinary(16)).toField("uuid"))
            .build();

    BatchSchema result = schemaConverter.fromIceberg(icebergSchema);
    assertEquals(result, schema);
  }

  @Test
  public void list() {
    BatchSchema schema =
        BatchSchema.newBuilder()
            .addField(INT.asList().toField("list_int"))
            .addField(INT.asList().asList().toField("list_list_int"))
            .build();

    Schema icebergSchema = schemaConverter.toIcebergSchema(schema);
    BatchSchema result = schemaConverter.fromIceberg(icebergSchema);
    assertEquals(result, schema);
  }

  @Test
  public void map() {
    SchemaConverter sc = SchemaConverter.getBuilder().setMapTypeEnabled(true).build();
    Field struct =
        CompleteType.struct(VARCHAR.toField("key", false), VARCHAR.toField("value"))
            .toField(MapVector.DATA_VECTOR_NAME, false);

    BatchSchema schema =
        BatchSchema.newBuilder()
            .addField(
                new Field(
                    "map",
                    new FieldType(true, CompleteType.MAP.getType(), null),
                    Arrays.asList(struct)))
            .build();

    Schema icebergSchema = sc.toIcebergSchema(schema);
    BatchSchema result = sc.fromIceberg(icebergSchema);
    assertEquals(result, schema);
  }

  @Test
  public void struct() {
    Field struct =
        CompleteType.struct(
                VARCHAR.toField("varchar"), INT.toField("int"), BIT.asList().toField("bits"))
            .toField("struct");

    BatchSchema schema = BatchSchema.newBuilder().addField(struct).build();

    Schema icebergSchema = schemaConverter.toIcebergSchema(schema);
    BatchSchema result = schemaConverter.fromIceberg(icebergSchema);
    assertEquals(result, schema);
  }

  @Test
  public void mixed() throws Exception {
    BatchSchema schema =
        BatchSchema.newBuilder()
            .addField(INT.toField("rownum"))
            .addField(VARCHAR.toField("name"))
            .addField(INT.toField("age"))
            .addField(FLOAT.toField("gpa"))
            .addField(BIGINT.toField("studentnum"))
            .addField(TIMESTAMP.toField("create_time"))
            .addField(VARCHAR.asList().toField("interests"))
            .addField(
                CompleteType.struct(
                        VARCHAR.toField("color"), VARCHAR.toField("sport"), VARCHAR.toField("food"))
                    .toField("favorites"))
            .build();

    Schema expectedSchema =
        new Schema(
            optional(1, "rownum", IntegerType.get()),
            optional(2, "name", StringType.get()),
            optional(3, "age", IntegerType.get()),
            optional(4, "gpa", FloatType.get()),
            optional(5, "studentnum", LongType.get()),
            optional(6, "create_time", TimestampType.withZone()),
            optional(7, "interests", ListType.ofOptional(9, StringType.get())),
            optional(
                8,
                "favorites",
                StructType.of(
                    optional(10, "color", StringType.get()),
                    optional(11, "sport", StringType.get()),
                    optional(12, "food", StringType.get()))));

    Schema icebergResult = schemaConverter.toIcebergSchema(schema);
    assertEquals(expectedSchema.toString(), icebergResult.toString());

    TemporaryFolder folder = new TemporaryFolder();
    folder.create();

    String rootPath = folder.getRoot().toString();
    Configuration conf = new Configuration();

    FileSystemPlugin fileSystemPlugin = BaseTestQuery.getMockedFileSystemPlugin();
    IcebergHadoopModel icebergHadoopModel = new IcebergHadoopModel(fileSystemPlugin);
    when(fileSystemPlugin.getIcebergModel()).thenReturn(icebergHadoopModel);

    IcebergOpCommitter createTableCommitter =
        icebergHadoopModel.getCreateTableCommitter(
            "testTableName",
            icebergHadoopModel.getTableIdentifier(rootPath),
            schema,
            Collections.emptyList(),
            null,
            null,
            null,
            Collections.emptyMap());
    createTableCommitter.commit();

    Table table = new HadoopTables(conf).load(rootPath);
    assertEquals(expectedSchema.toString(), table.schema().toString());
  }

  @Test
  public void testPartitionComparatorField() {
    BatchSchema inputschema =
        BatchSchema.newBuilder()
            .addField(BIT.toField("boolean"))
            .addField(INT.toField("int"))
            .addField(BIT.toField(WriterPrel.PARTITION_COMPARATOR_FIELD))
            .build();

    Schema expectedSchema =
        new Schema(
            optional(1, "boolean", BooleanType.get()), optional(2, "int", IntegerType.get()));

    assertEquals(
        schemaConverter.toIcebergSchema(inputschema).toString(), expectedSchema.toString());
  }

  @Test
  public void unsupportedArrowTypes() {
    BatchSchema inputSchema =
        BatchSchema.newBuilder()
            .addField(
                CompleteType.union(INT.toField("int_field"), BIGINT.toField("bigint_field"))
                    .toField("union_field"))
            .build();
    assertThatThrownBy(() -> schemaConverter.toIcebergSchema(inputSchema))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Type conversion error for column union_field");
  }

  @Test
  public void testUnboundedFieldIdBroker() {
    Schema icebergSchema =
        new Schema(
            optional(1, "boolean", BooleanType.get()),
            optional(2, "int", IntegerType.get()),
            optional(3, "long", LongType.get()),
            optional(4, "float", FloatType.get()),
            optional(5, "double", DoubleType.get()));

    BatchSchema batchSchema =
        BatchSchema.newBuilder()
            .addField(BIT.toField("boolean"))
            .addField(INT.toField("int"))
            .addField(BIGINT.toField("long"))
            .addField(FLOAT.toField("float"))
            .addField(DOUBLE.toField("double"))
            .build();

    assertEquals(batchSchema, schemaConverter.fromIceberg(icebergSchema));

    FieldIdBroker fieldIdBroker = new UnboundedFieldIdBroker();
    assertEquals(
        icebergSchema.toString(),
        TypeUtil.assignIncreasingFreshIds(
                schemaConverter.toIcebergSchema(batchSchema, fieldIdBroker))
            .toString());
  }

  @Test
  public void testSeededFieldIdBrokerForPrimitives() {
    Schema icebergSchema =
        new Schema(
            optional(1, "boolean", BooleanType.get()),
            optional(2, "int", IntegerType.get()),
            optional(3, "long", LongType.get()),
            optional(4, "float", FloatType.get()),
            optional(5, "double", DoubleType.get()));

    BatchSchema batchSchema =
        BatchSchema.newBuilder()
            .addField(BIT.toField("boolean"))
            .addField(INT.toField("int"))
            .addField(BIGINT.toField("long"))
            .addField(FLOAT.toField("float"))
            .addField(DOUBLE.toField("double"))
            .build();

    assertEquals(batchSchema, schemaConverter.fromIceberg(icebergSchema));

    Map<String, Integer> columnIdMap = IcebergUtils.getIcebergColumnNameToIDMap(icebergSchema);
    FieldIdBroker fieldIdBroker =
        new SeededFieldIdBroker(CaseInsensitiveImmutableBiMap.newImmutableMap(columnIdMap));
    assertEquals(
        icebergSchema.toString(),
        schemaConverter.toIcebergSchema(batchSchema, fieldIdBroker).toString());
  }

  @Test
  public void testSeededFieldIdBrokerForStruct() {
    BatchSchema batchSchema =
        BatchSchema.newBuilder()
            .addField(CompleteType.struct(VARCHAR.toField("varchar")).toField("struct"))
            .addField(
                CompleteType.struct(
                        CompleteType.struct(VARCHAR.toField("varchar")).toField("struct"))
                    .toField("struct_struct"))
            .addField(
                CompleteType.struct(
                        CompleteType.struct(VARCHAR.toField("varchar")).toField("struct"))
                    .asList()
                    .toField("list_struct_struct"))
            .build();
    Schema icebergSchema =
        new Schema(
            optional(1, "struct", StructType.of(optional(4, "varchar", StringType.get()))),
            optional(
                2,
                "struct_struct",
                StructType.of(
                    optional(
                        5, "struct", StructType.of(optional(6, "varchar", StringType.get()))))),
            optional(
                3,
                "list_struct_struct",
                ListType.ofOptional(
                    7,
                    StructType.of(
                        optional(
                            8,
                            "struct",
                            StructType.of(optional(9, "varchar", StringType.get())))))));
    assertEquals(batchSchema, schemaConverter.fromIceberg(icebergSchema));

    Map<String, Integer> columnIdMap = IcebergUtils.getIcebergColumnNameToIDMap(icebergSchema);
    FieldIdBroker fieldIdBroker =
        new SeededFieldIdBroker(CaseInsensitiveImmutableBiMap.newImmutableMap(columnIdMap));
    assertEquals(
        icebergSchema.toString(),
        schemaConverter.toIcebergSchema(batchSchema, fieldIdBroker).toString());
  }

  @Test
  public void testSeededFieldIdBrokerForList() {
    BatchSchema batchSchema =
        BatchSchema.newBuilder()
            .addField(INT.asList().toField("list"))
            .addField(INT.asList().asList().toField("list_list"))
            .addField(
                CompleteType.struct(
                        CompleteType.struct(VARCHAR.toField("varchar")).toField("struct"))
                    .asList()
                    .toField("list_struct_struct"))
            .build();
    Schema icebergSchema =
        new Schema(
            optional(1, "list", ListType.ofOptional(4, IntegerType.get())),
            optional(
                2, "list_list", ListType.ofOptional(5, ListType.ofOptional(6, IntegerType.get()))),
            optional(
                3,
                "list_struct_struct",
                ListType.ofOptional(
                    7,
                    StructType.of(
                        optional(
                            8,
                            "struct",
                            StructType.of(optional(9, "varchar", StringType.get())))))));
    assertEquals(batchSchema, schemaConverter.fromIceberg(icebergSchema));

    Map<String, Integer> columnIdMap = IcebergUtils.getIcebergColumnNameToIDMap(icebergSchema);
    FieldIdBroker fieldIdBroker =
        new SeededFieldIdBroker(CaseInsensitiveImmutableBiMap.newImmutableMap(columnIdMap));
    assertEquals(
        icebergSchema.toString(),
        schemaConverter.toIcebergSchema(batchSchema, fieldIdBroker).toString());
  }

  @Test
  public void testSeededFieldIdBrokerForMap() {
    Schema icebergSchema =
        new Schema(
            optional(
                4,
                "map",
                MapType.ofOptional(
                    2, 3, IntegerType.get(), ListType.ofOptional(1, StringType.get()))));
    Map<String, Integer> columnIdMap = IcebergUtils.getIcebergColumnNameToIDMap(icebergSchema);
    CaseInsensitiveImmutableBiMap<Integer> columnIdMapping =
        CaseInsensitiveImmutableBiMap.newImmutableMap(columnIdMap);

    // Name of key and value fields can only be named key and value respectively. As iceberg does
    // not store the field names of key and value in metadata.
    List<Field> children =
        Arrays.asList(INT.toField("key", false), VARCHAR.asList().toField("value"));
    Field mapField =
        new Field(
            "map",
            FieldType.nullable(CompleteType.MAP.getType()),
            Arrays.asList(
                CompleteType.struct(children).toField(MapVector.DATA_VECTOR_NAME, false)));
    Types.NestedField nestedField =
        schemaConverter.toIcebergColumn(mapField, new SeededFieldIdBroker(columnIdMapping));
    assertEquals(nestedField, icebergSchema.columns().get(0));
  }

  @Test
  public void testConversionToIcebergFields() {
    List<Types.NestedField> icebergFields =
        Arrays.asList(
            optional(0, "boolean", BooleanType.get()),
            optional(1, "int", IntegerType.get()),
            optional(2, "long", LongType.get()),
            optional(3, "float", FloatType.get()),
            optional(4, "double", DoubleType.get()),
            optional(5, "decimal_38_16", DecimalType.of(38, 16)),
            optional(6, "string", StringType.get()),
            optional(7, "binary", BinaryType.get()),
            optional(8, "date", DateType.get()),
            optional(9, "time", TimeType.get()),
            optional(10, "fixed_32", FixedType.ofLength(32)),
            optional(11, "timestamp", TimestampType.withZone()));

    BatchSchema schema =
        BatchSchema.newBuilder()
            .addField(BIT.toField("boolean"))
            .addField(INT.toField("int"))
            .addField(BIGINT.toField("long"))
            .addField(FLOAT.toField("float"))
            .addField(DOUBLE.toField("double"))
            .addField(new CompleteType(new Decimal(38, 16, 128)).toField("decimal_38_16"))
            .addField(VARCHAR.toField("string"))
            .addField(VARBINARY.toField("binary"))
            .addField(DATE.toField("date"))
            .addField(TIME.toField("time"))
            .addField(new CompleteType(new ArrowType.FixedSizeBinary(32)).toField("fixed_32"))
            .addField(TIMESTAMP.toField("timestamp"))
            .build();

    assertEquals(icebergFields, schemaConverter.toIcebergFields(schema.getFields()));
  }
}

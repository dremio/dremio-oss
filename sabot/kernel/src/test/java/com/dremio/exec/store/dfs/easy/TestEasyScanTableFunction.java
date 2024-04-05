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
package com.dremio.exec.store.dfs.easy;

import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.VARCHAR;
import static com.dremio.sabot.RecordSet.li;
import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.RecordSet.st;
import static org.apache.arrow.vector.types.pojo.ArrowType.Decimal.createDecimal;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertTrue;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.sabot.RecordBatchValidator;
import com.dremio.sabot.RecordBatchValidatorDefaultImpl;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;

public class TestEasyScanTableFunction extends BaseTestEasyScanTableFunction {

  private static final String ALL_PRIMITIVE_TYPE_OUTPUT =
      "/store/text/data/allprimitiveoutput.csvh";

  private static final BatchSchema OUTPUT_SCHEMA_ALL_TYPE =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable("col1", CompleteType.BIT.getType()),
              Field.nullable("col2", CompleteType.INT.getType()),
              Field.nullable("col3", CompleteType.BIGINT.getType()),
              Field.nullable("col4", CompleteType.FLOAT.getType()),
              Field.nullable("col5", CompleteType.DOUBLE.getType()),
              Field.nullable("col6", (new CompleteType(createDecimal(6, 2, null))).getType()),
              Field.nullable("col7", CompleteType.VARCHAR.getType())));

  private static final List<SchemaPath> ALL_TYPE_COLUMNS =
      ImmutableList.of(
          SchemaPath.getSimplePath("col1"),
          SchemaPath.getSimplePath("col2"),
          SchemaPath.getSimplePath("col3"),
          SchemaPath.getSimplePath("col4"),
          SchemaPath.getSimplePath("col5"),
          SchemaPath.getSimplePath("col6"),
          SchemaPath.getSimplePath("col7"));

  private static final List<SchemaPath> JSON_COMPLEX =
      ImmutableList.of(
          SchemaPath.getSimplePath("col1"),
          SchemaPath.getSimplePath("col2"),
          SchemaPath.getSimplePath("col5"));

  private static final BatchSchema JSON_COMPLEX_OUTPUT_SCHEMA =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable("col1", CompleteType.VARCHAR.getType()),
              CompleteType.struct(INT.toField("col3"), VARCHAR.toField("col4"))
                  .toField("col2", true),
              INT.asList().toField("col5")));

  private static final List<SchemaPath> CAR_COLUMNS =
      ImmutableList.of(
          SchemaPath.getSimplePath("year"),
          SchemaPath.getSimplePath("make"),
          SchemaPath.getSimplePath("model"),
          SchemaPath.getSimplePath("description"),
          SchemaPath.getSimplePath("price"));
  private static final List<SchemaPath> COLUMNS_YEAR_PRICE =
      ImmutableList.of(SchemaPath.getSimplePath("year"), SchemaPath.getSimplePath("price"));
  private static final List<SchemaPath> COLUMNS_MISMATCH =
      ImmutableList.of(
          SchemaPath.getSimplePath("col1"),
          SchemaPath.getSimplePath("col2"),
          SchemaPath.getSimplePath("col5"),
          SchemaPath.getSimplePath("col3"),
          SchemaPath.getSimplePath("col6"),
          SchemaPath.getSimplePath("col7"),
          SchemaPath.getSimplePath("col8"));
  private static final BatchSchema CAR_OUTPUT_SCHEMA =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable("year", CompleteType.VARCHAR.getType()),
              Field.nullable("make", CompleteType.VARCHAR.getType()),
              Field.nullable("model", CompleteType.VARCHAR.getType()),
              Field.nullable("description", CompleteType.VARCHAR.getType()),
              Field.nullable("price", CompleteType.VARCHAR.getType())));
  private static final BatchSchema OUTPUT_SCHEMA_WITH_YEAR_AND_PRICE =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable("year", CompleteType.VARCHAR.getType()),
              Field.nullable("price", CompleteType.VARCHAR.getType())));

  private static final BatchSchema OUTPUT_SCHEMA_COLUMN_MISMATCH =
      new BatchSchema(
          ImmutableList.of(
              Field.nullable("col1", CompleteType.BIT.getType()),
              Field.nullable("col2", CompleteType.INT.getType()),
              Field.nullable("col5", CompleteType.DOUBLE.getType()),
              Field.nullable("col3", CompleteType.BIGINT.getType()),
              Field.nullable("col6", (new CompleteType(createDecimal(6, 2, null))).getType()),
              Field.nullable("col7", CompleteType.VARCHAR.getType()),
              Field.nullable("col8", CompleteType.INT.getType())));
  private static final String DATA_FILE_CARS_WITH_HEADER = "/store/text/data/simple-car.csvh";
  private static final String DATA_FILE_ALL_TYPE_WITH_HEADER =
      "/store/text/data/allprimitivetype.csvh";
  private static final String DATA_FILE_WITH_ERROR = "/store/text/data/errortype.csvh";
  private static final String JSON_DATA_FILE_ALL_TYPE = "/store/text/data/jsonallprimitive.json";
  private static final String JSON_DATA_FILE_COMPLEX_TYPE = "/store/text/data/complexjson.json";
  private static final String EXTRA_COLUMN_JSON_DATA_FILE = "/store/text/data/extracolumnjson.json";

  private static final String ERROR_JSON_DATA_FILE = "/store/text/data/typeerrorjson.json";

  @Test
  public void testDataFileScan() throws Exception {
    mockTextFormatPlugin();
    RecordSet input =
        rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, inputRow(DATA_FILE_CARS_WITH_HEADER));
    RecordSet output =
        outputRecordSet(
            DATA_FILE_CARS_WITH_HEADER, OutputRecordType.ALL_CAR_COLUMNS, CAR_OUTPUT_SCHEMA, ",");
    validate(
        input,
        new RecordBatchValidatorDefaultImpl(output),
        CAR_COLUMNS,
        CAR_OUTPUT_SCHEMA,
        FileType.TEXT);
  }

  @Test
  public void testAllTypeDataFileScan() throws Exception {
    mockTextFormatPlugin();
    RecordSet input =
        rs(
            SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(DATA_FILE_ALL_TYPE_WITH_HEADER));
    RecordSet output =
        outputRecordSet(
            DATA_FILE_ALL_TYPE_WITH_HEADER,
            OutputRecordType.ALL_DATA_TYPE_COLUMNS,
            OUTPUT_SCHEMA_ALL_TYPE,
            ",");
    validate(
        input,
        new RecordBatchValidatorDefaultImpl(output),
        ALL_TYPE_COLUMNS,
        OUTPUT_SCHEMA_ALL_TYPE,
        FileType.TEXT);
  }

  @Test
  public void testColumnMismatchCSV() throws Exception {
    mockTextFormatPlugin();
    RecordSet input =
        rs(
            SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(DATA_FILE_ALL_TYPE_WITH_HEADER));
    RecordSet output =
        outputRecordSet(
            DATA_FILE_ALL_TYPE_WITH_HEADER,
            OutputRecordType.COLUMNS_MISMATCH,
            OUTPUT_SCHEMA_COLUMN_MISMATCH,
            ",");
    validate(
        input,
        new RecordBatchValidatorDefaultImpl(output),
        COLUMNS_MISMATCH,
        OUTPUT_SCHEMA_COLUMN_MISMATCH,
        FileType.TEXT);
  }

  @Test
  public void testOnError() throws Exception {
    mockTextFormatPlugin();
    RecordSet input =
        rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, inputRow(DATA_FILE_WITH_ERROR));
    RecordSet output =
        outputRecordSet(
            DATA_FILE_ALL_TYPE_WITH_HEADER,
            OutputRecordType.ALL_DATA_TYPE_COLUMNS,
            OUTPUT_SCHEMA_ALL_TYPE,
            ",");
    try {
      validate(
          input,
          new RecordBatchValidatorDefaultImpl(output),
          ALL_TYPE_COLUMNS,
          OUTPUT_SCHEMA_ALL_TYPE,
          FileType.TEXT);
    } catch (UserException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Error processing input: While processing field \"col1\". Could not convert \"f\" to BOOLEAN. Reason: Unsupported boolean type value : f"));
      assertTrue(e.getMessage().contains("store/text/data/errortype.csvh, line=2, char=91"));
    }
  }

  @Test
  public void testDataFileScanWithRandomColumnOrder() throws Exception {
    mockTextFormatPlugin();
    RecordSet input =
        rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, inputRow(DATA_FILE_CARS_WITH_HEADER));
    RecordSet output =
        outputRecordSet(
            DATA_FILE_CARS_WITH_HEADER,
            OutputRecordType.YEAR_PRICE,
            OUTPUT_SCHEMA_WITH_YEAR_AND_PRICE,
            ",");
    validate(
        input,
        new RecordBatchValidatorDefaultImpl(output),
        COLUMNS_YEAR_PRICE,
        OUTPUT_SCHEMA_WITH_YEAR_AND_PRICE,
        FileType.TEXT);
  }

  @Test
  public void testAllTypeJsonDataFileScan() throws Exception {
    mockJsonFormatPlugin();
    RecordSet input =
        rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, inputRow(JSON_DATA_FILE_ALL_TYPE));
    RecordSet output =
        outputRecordSet(
            ALL_PRIMITIVE_TYPE_OUTPUT,
            OutputRecordType.ALL_DATA_TYPE_COLUMNS,
            OUTPUT_SCHEMA_ALL_TYPE,
            ",");
    validate(
        input,
        new RecordBatchValidatorDefaultImpl(output),
        ALL_TYPE_COLUMNS,
        OUTPUT_SCHEMA_ALL_TYPE,
        FileType.JSON);
  }

  @Test
  public void testComplexJsonDataFileScan() throws Exception {
    mockJsonFormatPlugin();
    RecordSet input =
        rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, inputRow(JSON_DATA_FILE_COMPLEX_TYPE));
    RecordSet.Record r1 = r("abc", st(106, "str1"), li(5, 6, 7));
    RecordSet.Record r2 = r("pqr", st(108, "str2"), li(8, 9, 99));
    RecordSet output = rs(JSON_COMPLEX_OUTPUT_SCHEMA, r1, r2);
    validate(
        input,
        new RecordBatchValidatorDefaultImpl(output),
        JSON_COMPLEX,
        JSON_COMPLEX_OUTPUT_SCHEMA,
        FileType.JSON);
  }

  @Test
  public void testComplexJsonMapType() throws Exception {
    Schema icebergSchema =
        new Schema(
            optional(1, "col1", Types.StringType.get()),
            optional(
                2,
                "col2",
                Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.IntegerType.get())));

    BatchSchema fields =
        SchemaConverter.getBuilder().setMapTypeEnabled(true).build().fromIceberg(icebergSchema);

    List<SchemaPath> projected =
        ImmutableList.of(SchemaPath.getSimplePath("col1"), SchemaPath.getSimplePath("col2"));

    mockJsonFormatPlugin();
    RecordSet input =
        rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, inputRow(JSON_DATA_FILE_COMPLEX_TYPE));
    RecordSet.Record r1 = r("abc", st(106, "str1"));
    RecordSet.Record r2 = r("pqr", st(108, "str2"));
    RecordSet output = rs(fields, r1, r2);
    boolean exception = false;
    try {
      validate(
          input, new RecordBatchValidatorDefaultImpl(output), projected, fields, FileType.JSON);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("'COPY INTO Command' does not support MAP Type."));
      exception = true;
    }
    assertTrue(exception);
  }

  @Test
  public void testJsonDataFileScanWithExtraColAndMismatchOrder() throws Exception {
    mockJsonFormatPlugin();
    RecordSet input =
        rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, inputRow(EXTRA_COLUMN_JSON_DATA_FILE));
    RecordSet.Record r1 = r("abc", st(106, "str1"), li(5, 6, 7));
    RecordSet.Record r2 = r("pqr", st(108, "str2"), li(8, 9, 99));
    RecordSet output = rs(JSON_COMPLEX_OUTPUT_SCHEMA, r1, r2);
    RecordBatchValidatorDefaultImpl validator = new RecordBatchValidatorDefaultImpl(output);
    validate(input, validator, JSON_COMPLEX, JSON_COMPLEX_OUTPUT_SCHEMA, FileType.JSON);
  }

  @Test
  public void testTypeErrorJsonDataFileScan() throws Exception {
    mockJsonFormatPlugin();
    RecordSet input =
        rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, inputRow(ERROR_JSON_DATA_FILE));
    RecordSet output =
        outputRecordSet(
            ALL_PRIMITIVE_TYPE_OUTPUT,
            OutputRecordType.ALL_DATA_TYPE_COLUMNS,
            OUTPUT_SCHEMA_ALL_TYPE,
            ",");
    try {
      validate(
          input,
          new RecordBatchValidatorDefaultImpl(output),
          ALL_TYPE_COLUMNS,
          OUTPUT_SCHEMA_ALL_TYPE,
          FileType.JSON);
    } catch (UserException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Error parsing JSON - While processing field \"col2\". Could not convert \"abc\" to INT"));
      assertTrue(e.getMessage().contains("store/text/data/typeerrorjson.json Line: 12, Record: 2"));
    }
  }

  private void validate(
      RecordSet input,
      RecordBatchValidator output,
      List<SchemaPath> projectedColumns,
      BatchSchema batchSchema,
      FileType fileType)
      throws Exception {
    TableFunctionPOP pop = getPop(batchSchema, projectedColumns, fileType);
    validateSingle(pop, TableFunctionOperator.class, input, output, BATCH_SIZE);
  }

  private TableFunctionPOP getPop(
      BatchSchema fullSchema, List<SchemaPath> columns, FileType fileType) throws Exception {
    BatchSchema projectedSchema = fullSchema.maskAndReorder(columns);
    return new TableFunctionPOP(
        PROPS,
        null,
        getTableFunctionConfig(fullSchema, projectedSchema, columns, fileType, EXTENDED_PROPS));
  }
}

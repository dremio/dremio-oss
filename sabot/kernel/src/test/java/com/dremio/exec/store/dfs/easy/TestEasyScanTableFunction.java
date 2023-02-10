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
import static com.dremio.sabot.RecordSet.Record;
import static com.dremio.sabot.RecordSet.li;
import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.RecordSet.st;
import static org.apache.arrow.vector.types.pojo.ArrowType.Decimal.createDecimal;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.fn.impl.DecimalFunctions;
import com.dremio.exec.hadoop.HadoopCompressionCodecFactory;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.config.EasyScanTableFunctionContext;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.easy.EasyFormatUtils;
import com.dremio.exec.store.easy.json.JSONFormatPlugin;
import com.dremio.exec.store.easy.text.TextFormatPlugin;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.parquet.ParquetScanTableFunction;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

public class TestEasyScanTableFunction extends BaseTestTableFunction {
  private static final List<SchemaPath> CAR_COLUMNS = ImmutableList.of(
          SchemaPath.getSimplePath("year"),
          SchemaPath.getSimplePath("make"),
          SchemaPath.getSimplePath("model"),
          SchemaPath.getSimplePath("description"),
          SchemaPath.getSimplePath("price")
  );

  private static final List<SchemaPath> COLUMNS_YEAR_PRICE = ImmutableList.of(
          SchemaPath.getSimplePath("year"),
          SchemaPath.getSimplePath("price")
  );

  private static final List<SchemaPath> ALL_TYPE_COLUMNS = ImmutableList.of(
          SchemaPath.getSimplePath("col1"),
          SchemaPath.getSimplePath("col2"),
          SchemaPath.getSimplePath("col3"),
          SchemaPath.getSimplePath("col4"),
          SchemaPath.getSimplePath("col5"),
          SchemaPath.getSimplePath("col6"),
          SchemaPath.getSimplePath("col7")
  );

  private static final List<SchemaPath> COLUMNS_MISMATCH = ImmutableList.of(
          SchemaPath.getSimplePath("col1"),
          SchemaPath.getSimplePath("col2"),
          SchemaPath.getSimplePath("col5"),
          SchemaPath.getSimplePath("col3"),
          SchemaPath.getSimplePath("col6"),
          SchemaPath.getSimplePath("col7"),
          SchemaPath.getSimplePath("col8")
  );

  private static final List<SchemaPath> JSON_COMPLEX = ImmutableList.of(
          SchemaPath.getSimplePath("col1"),
          SchemaPath.getSimplePath("col2"),
          SchemaPath.getSimplePath("col5")
  );

  private static final int BATCH_SIZE = 67;

  private static final BatchSchema CAR_OUTPUT_SCHEMA = new BatchSchema(ImmutableList.of(
          Field.nullable("year", CompleteType.VARCHAR.getType()),
          Field.nullable("make", CompleteType.VARCHAR.getType()),
          Field.nullable("model", CompleteType.VARCHAR.getType()),
          Field.nullable("description", CompleteType.VARCHAR.getType()),
          Field.nullable("price", CompleteType.VARCHAR.getType())));
  private static final BatchSchema OUTPUT_SCHEMA_WITH_YEAR_AND_PRICE = new BatchSchema(ImmutableList.of(
          Field.nullable("year", CompleteType.VARCHAR.getType()),
          Field.nullable("price", CompleteType.VARCHAR.getType())));

  private static final BatchSchema OUTPUT_SCHEMA_ALL_TYPE = new BatchSchema(ImmutableList.of(
          Field.nullable("col1", CompleteType.BIT.getType()),
          Field.nullable("col2", CompleteType.INT.getType()),
          Field.nullable("col3", CompleteType.BIGINT.getType()),
          Field.nullable("col4", CompleteType.FLOAT.getType()),
          Field.nullable("col5", CompleteType.DOUBLE.getType()),
          Field.nullable("col6", (new CompleteType(createDecimal(6, 2, null))).getType()),
          Field.nullable("col7", CompleteType.VARCHAR.getType())));

  private static final BatchSchema OUTPUT_SCHEMA_COLUMN_MISMATCH = new BatchSchema(ImmutableList.of(
          Field.nullable("col1", CompleteType.BIT.getType()),
          Field.nullable("col2", CompleteType.INT.getType()),
          Field.nullable("col5", CompleteType.DOUBLE.getType()),
          Field.nullable("col3", CompleteType.BIGINT.getType()),
          Field.nullable("col6", (new CompleteType(createDecimal(6, 2, null))).getType()),
          Field.nullable("col7", CompleteType.VARCHAR.getType()),
          Field.nullable("col8", CompleteType.INT.getType())));

  private static final BatchSchema JSON_COMPLEX_OUTPUT_SCHEMA = new BatchSchema(ImmutableList.of(
          Field.nullable("col1", CompleteType.VARCHAR.getType()),
          CompleteType.struct(
                  INT.toField("col3"),
                  VARCHAR.toField("col4")).toField("col2", true),
          INT.asList().toField("col5")));
  private static final List<String> PARTITION_COLUMNS = ImmutableList.of();
  private static final ByteString EXTENDED_PROPS = ByteString.EMPTY;
  private static String DATA_FILE_CARS_WITH_HEADER = "/store/text/data/simple-car.csvh";
  private static String DATA_FILE_ALL_TYPE_WITH_HEADER = "/store/text/data/allprimitivetype.csvh";
  private static String DATA_FILE_WITH_ERROR = "/store/text/data/errortype.csvh";
  private static String JSON_DATA_FILE_ALL_TYPE = "/store/text/data/jsonallprimitive.json";
  private static String JSON_DATA_FILE_PRIMITIVE_TYPE_OUTPUT = "/store/text/data/jsonallprimitiveoutput.csvh";
  private static String JSON_DATA_FILE_COMPLEX_TYPE = "/store/text/data/complexjson.json";
  private static String EXTRA_COLUMN_JSON_DATA_FILE = "/store/text/data/extracolumnjson.json";

  private static String ERROR_JSON_DATA_FILE = "/store/text/data/typeerrorjson.json";

  private static final PartitionProtobuf.NormalizedPartitionInfo PARTITION_INFO =
          PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
                  .setId("1")
                  .addValues(PartitionProtobuf.PartitionValue.newBuilder()
                          .build())
                  .build();

  SabotContext sobotContex = mock(SabotContext.class);

  private enum OutputRecordType {
    ALL_CAR_COLUMNS,
    YEAR_PRICE,
    ALL_DATA_TYPE_COLUMNS,
    COLUMNS_MISMATCH
  }


  private FileSystem fs;
  @Mock
  private StoragePluginId pluginId;

  @Mock(extraInterfaces = {SupportsIcebergRootPointer.class, SupportsInternalIcebergTable.class, MutablePlugin.class})
  private FileSystemPlugin plugin;

  @Before
  public void prepareMocks() throws Exception {
    fs = HadoopFileSystem.get(Path.of("/"), new Configuration());
    when(fec.getStoragePlugin(pluginId)).thenReturn(plugin);
    SupportsIcebergRootPointer sirp = plugin;
    when(sirp.createFSWithAsyncOptions(anyString(), anyString(), any())).thenReturn(fs);
    SupportsInternalIcebergTable siit = plugin;
    when(plugin.createFS(any(), any(), any())).thenReturn(fs);
    when(plugin.getCompressionCodecFactory()).thenReturn(new HadoopCompressionCodecFactory(new Configuration()));
    when(siit.createScanTableFunction(any(), any(), any(), any())).thenAnswer(i ->
            new ParquetScanTableFunction(i.getArgument(0), i.getArgument(1), i.getArgument(2), i.getArgument(3)));
    when(pluginId.getName()).thenReturn("testpluginEasyScan");
  }

  @Test
  public void testDataFileScan() throws Exception {
    mockTextFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(DATA_FILE_CARS_WITH_HEADER));
    RecordSet output = outputRecordSet(DATA_FILE_CARS_WITH_HEADER, OutputRecordType.ALL_CAR_COLUMNS, CAR_OUTPUT_SCHEMA);
    validate(input, output, CAR_COLUMNS, CAR_OUTPUT_SCHEMA, FileType.TEXT);
  }

  @Test
  public void testAllTypeDataFileScan() throws Exception {
    mockTextFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(DATA_FILE_ALL_TYPE_WITH_HEADER));
    RecordSet output = outputRecordSet(DATA_FILE_ALL_TYPE_WITH_HEADER, OutputRecordType.ALL_DATA_TYPE_COLUMNS, OUTPUT_SCHEMA_ALL_TYPE);
    validate(input, output, ALL_TYPE_COLUMNS, OUTPUT_SCHEMA_ALL_TYPE, FileType.TEXT);
  }

  @Test
  public void testColumnMismatchCSV() throws Exception {
    mockTextFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(DATA_FILE_ALL_TYPE_WITH_HEADER));
    RecordSet output = outputRecordSet(DATA_FILE_ALL_TYPE_WITH_HEADER, OutputRecordType.COLUMNS_MISMATCH, OUTPUT_SCHEMA_COLUMN_MISMATCH);
    validate(input, output, COLUMNS_MISMATCH, OUTPUT_SCHEMA_COLUMN_MISMATCH, FileType.TEXT);
  }

  @Test
  public void testOnError() throws Exception {
    mockTextFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(DATA_FILE_WITH_ERROR));
    RecordSet output = outputRecordSet(DATA_FILE_ALL_TYPE_WITH_HEADER, OutputRecordType.ALL_DATA_TYPE_COLUMNS, OUTPUT_SCHEMA_ALL_TYPE);
    try {
      validate(input, output, ALL_TYPE_COLUMNS, OUTPUT_SCHEMA_ALL_TYPE, FileType.TEXT);
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("Error processing input: While processing field \"col1\". Could not convert \"f\" to BOOLEAN. Reason: Unsupported boolean type value : f"));
      assertTrue(e.getMessage().contains("store/text/data/errortype.csvh, line=2, char=91"));
    }
  }

  @Test
  public void testDataFileScanWithRandomColumnOrder() throws Exception {
    mockTextFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(DATA_FILE_CARS_WITH_HEADER));
    RecordSet output = outputRecordSet(DATA_FILE_CARS_WITH_HEADER, OutputRecordType.YEAR_PRICE, OUTPUT_SCHEMA_WITH_YEAR_AND_PRICE);
    validate(input, output, COLUMNS_YEAR_PRICE, OUTPUT_SCHEMA_WITH_YEAR_AND_PRICE, FileType.TEXT);
  }

  @Test
  public void testAllTypeJsonDataFileScan() throws Exception {
    mockJsonFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(JSON_DATA_FILE_ALL_TYPE));
    RecordSet output = outputRecordSet(JSON_DATA_FILE_PRIMITIVE_TYPE_OUTPUT, OutputRecordType.ALL_DATA_TYPE_COLUMNS, OUTPUT_SCHEMA_ALL_TYPE);
    validate(input, output, ALL_TYPE_COLUMNS, OUTPUT_SCHEMA_ALL_TYPE, FileType.JSON);
  }

  @Test
  public void testComplexJsonDataFileScan() throws Exception {
    mockJsonFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(JSON_DATA_FILE_COMPLEX_TYPE));
    RecordSet.Record r1 = r("abc", st(106, "str1"), li(5,6,7));
    RecordSet.Record r2 = r("pqr", st(108, "str2"), li(8,9,99));
    RecordSet output = rs(JSON_COMPLEX_OUTPUT_SCHEMA, new RecordSet.Record[]{r1, r2});
    validate(input, output, JSON_COMPLEX, JSON_COMPLEX_OUTPUT_SCHEMA, FileType.JSON);
  }

  @Test
  public void testComplexJsonMapType() throws Exception {
    Schema icebergSchema = new Schema(
            optional(1, "col1", Types.StringType.get()),
            optional(2, "col2", Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.IntegerType.get()))
    );

    BatchSchema fields = SchemaConverter.getBuilder().setMapTypeEnabled(true).build().fromIceberg(icebergSchema);

    List<SchemaPath> projected = ImmutableList.of(
            SchemaPath.getSimplePath("col1"),
            SchemaPath.getSimplePath("col2")
    );

    mockJsonFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(JSON_DATA_FILE_COMPLEX_TYPE));
    RecordSet.Record r1 = r("abc", st(106, "str1"));
    RecordSet.Record r2 = r("pqr", st(108, "str2"));
    RecordSet output = rs(fields, new RecordSet.Record[]{r1, r2});
    boolean exception = false;
    try {
      validate(input, output, projected, fields, FileType.JSON);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("'COPY INTO Command' does not support MAP Type."));
      exception = true;
    }
    assertTrue(exception);
  }

  @Test
  public void testJsonDataFileScanWithExtraColAndMismatchOrder() throws Exception {
    mockJsonFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(EXTRA_COLUMN_JSON_DATA_FILE));
    RecordSet.Record r1 = r("abc", st(106, "str1"), li(5,6,7));
    RecordSet.Record r2 = r("pqr", st(108, "str2"), li(8,9,99));
    RecordSet output = rs(JSON_COMPLEX_OUTPUT_SCHEMA, new RecordSet.Record[]{r1, r2});
    validate(input, output, JSON_COMPLEX, JSON_COMPLEX_OUTPUT_SCHEMA, FileType.JSON);
  }

  @Test
  public void testErrorJsonDataFileScan() throws Exception {
    mockJsonFormatPlugin();
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            inputRow(ERROR_JSON_DATA_FILE));
    RecordSet output = outputRecordSet(JSON_DATA_FILE_PRIMITIVE_TYPE_OUTPUT, OutputRecordType.ALL_DATA_TYPE_COLUMNS, OUTPUT_SCHEMA_ALL_TYPE);
    try {
      validate(input, output, ALL_TYPE_COLUMNS, OUTPUT_SCHEMA_ALL_TYPE, FileType.JSON);
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("Error parsing JSON - While processing field \"col2\". Could not convert \"abc\" to INT"));
      assertTrue(e.getMessage().contains("store/text/data/typeerrorjson.json Line: 12, Record: 2"));
    }
  }

  public void mockJsonFormatPlugin() {
    when(plugin.getFormatPlugin((FormatPluginConfig) any())).thenReturn(new JSONFormatPlugin("json", sobotContex, plugin));
  }

  public void mockTextFormatPlugin() throws Exception {
    when(plugin.getFormatPlugin((FormatPluginConfig) any())).thenReturn(new TextFormatPlugin("text", sobotContex, (TextFormatPlugin.TextFormatConfig) PhysicalDatasetUtils.toFormatPlugin(getFileConfig(FileType.TEXT), Collections.<String>emptyList()), plugin));
  }
  private RecordSet outputRecordSet(String relativePath, OutputRecordType recordType, BatchSchema batchSchema) throws Exception {
    List<RecordSet.Record> rows = new ArrayList<>();
    String path = FileUtils.getResourceAsFile(relativePath).toString();
    BufferedReader br = new BufferedReader(new FileReader(path));
    String line;
    Boolean isHeader = true;
    while ((line = br.readLine()) != null)   //returns a Boolean value
    {
      if(isHeader) {
        isHeader = false;
        continue;
      }
      String[] colValues = line.split(",");
      rows.add(getRecord(colValues, recordType));
    }
    return rs(batchSchema,
            rows.toArray(new RecordSet.Record[0]));
  }

  Record getRecord(String[] colValue , OutputRecordType recordType) {
    // year, make ,model ,description, price
    switch (recordType) {
      case YEAR_PRICE:
        return r(colValue[0], colValue[4]);
      case ALL_DATA_TYPE_COLUMNS:
        return r(EasyFormatUtils.TextBooleanFunction.apply(colValue[0]), Integer.valueOf(colValue[1]), Long.valueOf(colValue[2]), Float.valueOf(colValue[3]), Double.valueOf(colValue[4]), getDecimalValue(colValue[5]), colValue[6]);
      case COLUMNS_MISMATCH:
        return r(EasyFormatUtils.TextBooleanFunction.apply(colValue[0]), Integer.valueOf(colValue[1]), Double.valueOf(colValue[4]), Long.valueOf(colValue[2]), getDecimalValue(colValue[5]), colValue[6], null);
      default:
        return r(colValue[0], colValue[1], colValue[2], colValue[3], colValue[4]);
    }
  }

  private Object getDecimalValue(String value) {
    BigDecimal bd = new BigDecimal(value).setScale(2, java.math.RoundingMode.HALF_UP);
    // Decimal value will be 0 if there is precision overflow.
    // This is similar to DecimalFunctions:CastDecimalDecimal
    if (DecimalFunctions.checkOverflow(bd, 6)) {
      bd = new java.math.BigDecimal("0.0").setScale(2);
    }
    return bd;
  }

  private RecordSet.Record inputRow(String relativePath) throws Exception {
    return inputRow(relativePath, 0, -1);
  }

  private RecordSet.Record inputRow(String relativePath, long offset, long length) throws Exception {
    Path path = Path.of(FileUtils.getResourceAsFile(relativePath).toURI().toString());
    long fileSize = fs.getFileAttributes(path).size();
    if (length == -1) {
      length = fileSize;
    }
    return r(
            st(path.toString(), 0L, fileSize, fileSize),
            createSplitInformation(path.toString(), offset, length, fileSize, 0),
            EXTENDED_PROPS.toByteArray());
  }

  private static byte[] createSplitInformation(String path, long offset, long length, long fileSize, long mtime) throws Exception {
    EasyProtobuf.EasyDatasetSplitXAttr splitExtended = EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
            .setPath(path)
            .setStart(offset)
            .setLength(length)
            .build();
    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
            .setExtendedProperty(splitExtended.toByteString());
    return IcebergSerDe.serializeToByteArray(new SplitAndPartitionInfo(null, splitInfo.build()));
  }

  private void validate(RecordSet input, RecordSet output, List<SchemaPath> projectedColumns, BatchSchema batchSchema, FileType fileType) throws Exception {
    TableFunctionPOP pop = getPop(batchSchema, projectedColumns, PARTITION_COLUMNS,
            EXTENDED_PROPS, fileType);
    validateSingle(pop, TableFunctionOperator.class, input, output, BATCH_SIZE);
  }

  private TableFunctionPOP getPop(
          BatchSchema fullSchema,
          List<SchemaPath> columns,
          List<String> partitionColumns,
          ByteString extendedProps, FileType fileType) throws Exception {
    BatchSchema projectedSchema = fullSchema.maskAndReorder(columns);
    return new TableFunctionPOP(
            PROPS,
            null,
            new TableFunctionConfig(
                    TableFunctionConfig.FunctionType.EASY_DATA_FILE_SCAN,
                    false,
                    new EasyScanTableFunctionContext(
                            getFileConfig(fileType),
                            fullSchema,
                            projectedSchema,
                            null,
                            null,
                            pluginId,
                            null,
                            columns,
                            partitionColumns,
                            null,
                            extendedProps,
                            false,
                            false,
                            false,
                            null,
                            new ExtendedFormatOptions(false, false, null, null, null, null))));
  }

  private FileConfig getFileConfig(FileType fileType) throws Exception {
    switch (fileType) {
      case TEXT:
        TextFileConfig textFileConfig = new TextFileConfig();
        textFileConfig.setExtractHeader(true);
        textFileConfig.setLineDelimiter(ByteString.stringDefaultValue("\n"));
        return textFileConfig.asFileConfig();
      case JSON:
        return new JsonFileConfig().asFileConfig();
      default:
        throw new Exception("Not supported");
    }
  }
}

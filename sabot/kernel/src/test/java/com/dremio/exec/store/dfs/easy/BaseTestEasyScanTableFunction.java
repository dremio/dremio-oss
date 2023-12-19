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

import static com.dremio.sabot.RecordSet.Record;
import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.RecordSet.st;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.mockito.Mock;

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
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.exec.store.easy.EasyFormatUtils;
import com.dremio.exec.store.easy.json.JSONFormatPlugin;
import com.dremio.exec.store.easy.text.TextFormatPlugin;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.parquet.ParquetScanTableFunction;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.JsonFileConfig;
import com.dremio.service.namespace.file.proto.TextFileConfig;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

public abstract class BaseTestEasyScanTableFunction extends BaseTestTableFunction {

  protected static final ByteString EXTENDED_PROPS = ByteString.EMPTY;
  private static final List<String> PARTITION_COLUMNS = ImmutableList.of();
  protected static final int BATCH_SIZE = 67;

  private final SabotContext sobotContex = mock(SabotContext.class);
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

  protected void mockJsonFormatPlugin() {
    when(plugin.getFormatPlugin((FormatPluginConfig) any())).thenReturn(new JSONFormatPlugin("json", sobotContex, plugin));
  }

  protected void mockTextFormatPlugin() throws Exception {
    when(plugin.getFormatPlugin((FormatPluginConfig) any())).thenReturn(new TextFormatPlugin("text", sobotContex, (TextFormatPlugin.TextFormatConfig) PhysicalDatasetUtils.toFormatPlugin(getFileConfig(FileType.TEXT), Collections.emptyList()), plugin));
  }

  private StoragePluginId getPluginId() {
    return pluginId;
  }

  protected RecordSet.Record inputRow(String relativePath) throws Exception {
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
      createSplitInformation(path.toString(), offset, length),
      EXTENDED_PROPS.toByteArray());
  }

  private static byte[] createSplitInformation(String path, long offset, long length) throws Exception {
    EasyProtobuf.EasyDatasetSplitXAttr splitExtended = EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
      .setPath(path)
      .setStart(offset)
      .setLength(length)
      .build();
    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
      .setExtendedProperty(splitExtended.toByteString());
    return IcebergSerDe.serializeToByteArray(new SplitAndPartitionInfo(null, splitInfo.build()));
  }

  protected RecordSet outputRecordSet(String relativePath, OutputRecordType recordType, BatchSchema batchSchema, String valueDelimiter) throws Exception {
    List<RecordSet.Record> rows = new ArrayList<>();
    String path = FileUtils.getResourceAsFile(relativePath).toString();
    BufferedReader br = new BufferedReader(new FileReader(path));
    String line;
    boolean isHeader = true;
    while ((line = br.readLine()) != null)   //returns a Boolean value
    {
      if(isHeader) {
        isHeader = false;
        continue;
      }
      String[] colValues = line.split(valueDelimiter);
      rows.add(getRecord(colValues, recordType));
    }
    return rs(batchSchema,
      rows.toArray(new RecordSet.Record[0]));
  }


  private Record getRecord(String[] colValue , OutputRecordType recordType) throws Exception {
    // year, make ,model ,description, price
    switch (recordType) {
      case YEAR_PRICE:
        return r(colValue[0], colValue[4]);
      case ALL_DATA_TYPE_COLUMNS:
        return r(EasyFormatUtils.TextBooleanFunction.apply(colValue[0]), Integer.valueOf(colValue[1]), Long.valueOf(colValue[2]), Float.valueOf(colValue[3]), Double.valueOf(colValue[4]), getDecimalValue(colValue[5]), colValue[6]);
      case ALL_DATA_TYPE_COLUMNS_WITH_ERROR:
        return r(convertValues(colValue));
      case COLUMNS_MISMATCH:
        return r(EasyFormatUtils.TextBooleanFunction.apply(colValue[0]), Integer.valueOf(colValue[1]), Double.valueOf(colValue[4]), Long.valueOf(colValue[2]), getDecimalValue(colValue[5]), colValue[6], null);
      default:
        return r(colValue[0], colValue[1], colValue[2], colValue[3], colValue[4]);
    }
  }

  /**
   * Convert an array of column values from their string representations to corresponding Java data types.
   *
   * This method takes an array of column values represented as strings and converts them to their corresponding Java data types.
   * The method processes each column value based on its position in the array and applies the appropriate conversion logic.
   * If a column value is considered as "null" according to the {@link #isNull(String)} method, it will be set to null in the result array.
   * Otherwise, the method converts the column value to the appropriate data type based on the column's position and data type.
   *
   * @param colValues The array of column values represented as strings.
   * @return An array of objects representing the converted values with corresponding Java data types.
   * @throws Exception If an error occurs during the conversion process, or an invalid column ID is encountered.
   */
  private Object[] convertValues(String[] colValues) throws Exception {
    Object[] result = new Object[colValues.length];
    for (int i = 0; i < colValues.length; i++) {
      String colValue = colValues[i];
      if (isNull(colValue)) {
        result[i] = null;
        continue;
      }
      switch (i) {
        case 0:
          result[i] = EasyFormatUtils.TextBooleanFunction.apply(colValue);
          break;
        case 1:
          result[i] = Integer.parseInt(colValue);
          break;
        case 2:
          result[i] = Long.valueOf(colValue);
          break;
        case 3:
          result[i] = Float.valueOf(colValue);
          break;
        case 4:
          result[i] = Double.valueOf(colValue);
          break;
        case 5:
          result[i] = getDecimalValue(colValue);
          break;
        case 6:
        case 7:
          result[i] = colValue;
          break;
        default:
          throw new Exception("Invalid column id");
      }
    }
    return result;
  }

  /**
   * Checks if the given column value is considered as "null".
   *
   * This method checks if the provided column value is considered as "null". It performs a case-insensitive comparison
   * between the column value and the string "null" to determine if the value should be considered as null.
   *
   * @param colValue The column value to check for null.
   * @return {@code true} if the column value is considered as "null"; {@code false} otherwise.
   */
  private boolean isNull(String colValue) {
      return "null".equalsIgnoreCase(colValue);
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


  private FileConfig getFileConfig(FileType fileType) throws Exception {
    switch (fileType) {
      case TEXT:
      case CSV:
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

  protected TableFunctionConfig getTableFunctionConfig(BatchSchema fullSchema, BatchSchema projectedSchema,
                                                     List<SchemaPath> columns, FileType fileType, ByteString extededProps) throws Exception{
    return new TableFunctionConfig(
      TableFunctionConfig.FunctionType.EASY_DATA_FILE_SCAN,
      false,
      new EasyScanTableFunctionContext(
        getFileConfig(fileType),
        fullSchema,
        projectedSchema,
        null,
        null,
        getPluginId(),
        null,
        columns,
        PARTITION_COLUMNS,
        null,
        extededProps,
        false,
        false,
        false,
        null,
        new ExtendedFormatOptions(false, false, null, null, null, null)));
  }

  protected enum OutputRecordType {
    ALL_CAR_COLUMNS,
    YEAR_PRICE,
    ALL_DATA_TYPE_COLUMNS,
    ALL_DATA_TYPE_COLUMNS_WITH_ERROR,
    COLUMNS_MISMATCH
  }
}

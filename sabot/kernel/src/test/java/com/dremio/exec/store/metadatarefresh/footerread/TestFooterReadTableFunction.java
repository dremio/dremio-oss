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
package com.dremio.exec.store.metadatarefresh.footerread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.config.FooterReaderTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.util.VectorUtil;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

/**
 * Tests for {@link FooterReadTableFunction}
 */
public class TestFooterReadTableFunction extends BaseTestQuery {

  private static FileSystem fs;
  private static long currentTime;
  private VectorContainer incoming;
  private VectorAccessible outgoing;
  private IncomingRowConsumer<String, Long, Long, Integer, Boolean> incomingRow;
  private VarBinaryVector partitionVector;

  @BeforeClass
  public static void initialise() throws IOException {
    fs = HadoopFileSystem.getLocal(new Configuration());
    currentTime = System.currentTimeMillis();
  }

  @Before
  public void setupInputs() throws URISyntaxException {
    VarCharVector pathVector = new VarCharVector(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH, allocator);
    BigIntVector sizeVector = new BigIntVector(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_SIZE, allocator);
    BigIntVector mtimeVector = new BigIntVector(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.MODIFICATION_TIME, allocator);
    BitVector isDeletedFile = new BitVector(MetadataRefreshExecConstants.IS_DELETED_FILE, allocator);
    partitionVector = new VarBinaryVector(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.PARTITION_INFO, allocator);
    incoming = new VectorContainer();
    List<ValueVector> incomingVectors = ImmutableList.of(pathVector, sizeVector, mtimeVector, partitionVector, isDeletedFile);
    incoming.addCollection(incomingVectors);
    incomingVectors.forEach(ValueVector::allocateNew);

    incomingRow = (path, size, mtime, index, isAdded) -> {
      pathVector.set(index, path.getBytes(StandardCharsets.UTF_8));
      sizeVector.set(index, size);
      mtimeVector.set(index, mtime);
      isDeletedFile.set(index, isAdded?0:1);
    };
  }

  @After
  public void close() throws Exception {
    incoming.close();
    AutoCloseables.close(outgoing);
  }

  @Test
  public void testFooterReadTableFunctionForFSTables() throws URISyntaxException, ExecutionSetupException {
    FooterReadTableFunction tableFunction = new FooterReadTableFunction(getFragmentExecutionContext(), getOpCtx(), null, getConfig(null, FileType.PARQUET));
    tableFunction.setFs(fs);
    AtomicInteger counter = new AtomicInteger(0);
    incomingRow.accept(getFullPath("int96.parquet", FileType.PARQUET), 431L, currentTime, counter.getAndIncrement(),true);
    incomingRow.accept(getFullPath("decimals.parquet", FileType.PARQUET), 12219L, currentTime,counter.getAndIncrement(), true);
    incomingRow.accept(getFullPath("intTypes/int_8.parquet", FileType.PARQUET), 343L, currentTime, counter.getAndIncrement(), true);
    incomingRow.accept(getFullPath("empty.parquet", FileType.PARQUET), 0L, currentTime, counter.getAndIncrement(), true);
    incomingRow.accept(getFullPath("complex.parquet", FileType.PARQUET), 817L, currentTime, counter.getAndIncrement(), true);
    incomingRow.accept(getFullPath("arrl_2.parquet", FileType.PARQUET), 1062L, currentTime, counter.getAndIncrement(), true); // this requires reading records to figure out schema

    try {
      BatchSchema file0Schema = BatchSchema.of(
        Field.nullablePrimitive("varchar_field", new ArrowType.PrimitiveType.Utf8()),
        Field.nullablePrimitive("dir0", new ArrowType.PrimitiveType.Utf8()),
        Field.nullablePrimitive("dir1", new ArrowType.PrimitiveType.Utf8()),
        Field.nullablePrimitive("timestamp_field", new ArrowType.PrimitiveType.Timestamp(TimeUnit.MILLISECOND, null)));
      PartitionSpec file0PartitionSpec = IcebergUtils.getIcebergPartitionSpec(file0Schema, Arrays.asList("dir0", "dir1"), null);
      IcebergPartitionData file0PartitionData = new IcebergPartitionData(file0PartitionSpec.partitionType());
      file0PartitionData.setString(0, "partition0");
      file0PartitionData.setString(1, "partition1");
      partitionVector.set(0, IcebergSerDe.serializeToByteArray(file0PartitionData));

      BatchSchema file2Schema = BatchSchema.of(
        Field.nullablePrimitive("value", new ArrowType.PrimitiveType.Int(32, true)),
        Field.nullablePrimitive("dir0", new ArrowType.PrimitiveType.Int(32, true)),
        Field.nullablePrimitive("dir1", new ArrowType.PrimitiveType.Int(32, true)),
        Field.nullablePrimitive("index", new ArrowType.PrimitiveType.Int(32, true)));

      PartitionSpec file2PartitionSpec = IcebergUtils.getIcebergPartitionSpec(file2Schema, Arrays.asList("dir0", "dir1"), null);
      IcebergPartitionData file2PartitionData = new IcebergPartitionData(file2PartitionSpec.partitionType());
      file2PartitionData.setInteger(0, 324);
      file2PartitionData.setInteger(1, 189);
      partitionVector.set(2, IcebergSerDe.serializeToByteArray(file2PartitionData));

      incoming.setAllCount(7);
      incoming.buildSchema();
      outgoing = tableFunction.setup(incoming);

      VarBinaryVector outputDatafileVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing,
        MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.DATA_FILE);

      VarBinaryVector outputSchemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing,
        MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);

      tableFunction.startRow(0);
      assertEquals(1, tableFunction.processRow(0, 5));

      verifyOutput(outputDatafileVector.get(0), outputSchemaVector.get(0), file0Schema, file0PartitionData, IcebergMetadataInformation.IcebergMetadataFileType.ADD_DATAFILE);

      assertEquals(0, tableFunction.processRow(1, 5)); // one input row returns at most output 1 row
      tableFunction.closeRow();

      //Validate output for Deleted File at index 0
      BatchSchema deletedFile0Schema = BatchSchema.of(
              Field.nullablePrimitive("dir0", new ArrowType.PrimitiveType.Utf8()),
              Field.nullablePrimitive("dir1", new ArrowType.PrimitiveType.Utf8()));
      incomingRow.accept(getFullPath("int96.parquet", FileType.PARQUET), 431L, currentTime, 0, false);
      tableFunction.startRow(0);
      assertEquals(1, tableFunction.processRow(0, 5));
      verifyOutput(outputDatafileVector.get(0), outputSchemaVector.get(0), deletedFile0Schema, file0PartitionData, IcebergMetadataInformation.IcebergMetadataFileType.DELETE_DATAFILE);
      tableFunction.closeRow();

      tableFunction.startRow(1);
      assertEquals(1, tableFunction.processRow(1, 5));
      verifyOutput(outputDatafileVector.get(1), outputSchemaVector.get(1),
        BatchSchema.of(Field.nullable("EXPR$0", new ArrowType.Decimal(32, 20))),
        new IcebergPartitionData(PartitionSpec.unpartitioned().partitionType()), IcebergMetadataInformation.IcebergMetadataFileType.ADD_DATAFILE);
      tableFunction.closeRow();

      //Validate output for Deleted File at index 1
      incomingRow.accept(getFullPath("decimals.parquet", FileType.PARQUET), 12219L, currentTime, 1, false);
      tableFunction.startRow(1);
      assertEquals(1, tableFunction.processRow(1, 5));
      verifyOutput(outputDatafileVector.get(1), outputSchemaVector.get(1), BatchSchema.EMPTY, new IcebergPartitionData(PartitionSpec.unpartitioned().partitionType()), IcebergMetadataInformation.IcebergMetadataFileType.DELETE_DATAFILE);
      tableFunction.closeRow();

      tableFunction.startRow(2);
      assertEquals(1, tableFunction.processRow(2, 5));
      verifyOutput(outputDatafileVector.get(2), outputSchemaVector.get(2), file2Schema, file2PartitionData, IcebergMetadataInformation.IcebergMetadataFileType.ADD_DATAFILE);
      tableFunction.closeRow();

      //Validate output for Deleted File at index 2
      BatchSchema deletedFile2Schema = BatchSchema.of(
              Field.nullablePrimitive("dir0", new ArrowType.PrimitiveType.Int(32, true)),
              Field.nullablePrimitive("dir1", new ArrowType.PrimitiveType.Int(32, true)));
      incomingRow.accept(getFullPath("intTypes/int_8.parquet", FileType.PARQUET), 343L, currentTime, 2, false);
      tableFunction.startRow(2);
      assertEquals(1, tableFunction.processRow(2, 5));
      verifyOutput(outputDatafileVector.get(2), outputSchemaVector.get(2), deletedFile2Schema, file2PartitionData, IcebergMetadataInformation.IcebergMetadataFileType.DELETE_DATAFILE);
      tableFunction.closeRow();

      // empty parquet, outputs 0 rows
      tableFunction.startRow(3);
      assertEquals(0, tableFunction.processRow(3, 5));
      tableFunction.closeRow();

      tableFunction.startRow(4);
      assertEquals(1, tableFunction.processRow(3, 5));
      tableFunction.closeRow();

      tableFunction.startRow(5);
      assertEquals(1, tableFunction.processRow(4, 5));
      tableFunction.closeRow();

    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unable to read footer for file"));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testFooterReadTableFunctionForHiveTables() throws URISyntaxException, ExecutionSetupException {
    incomingRow.accept(getFullPath("int96.parquet", FileType.PARQUET), 431L, currentTime, 0, true);
    // For hive case, we are setting the table schema to later check that outputSchemaVector returns the same schema
    // and doesn't learns the schema from the input file.
    BatchSchema tableSchema = BatchSchema.EMPTY;
    TableFunctionConfig tableFunctionConfig = getConfig(tableSchema, FileType.PARQUET);
    FooterReadTableFunction tableFunction = new FooterReadTableFunction(getFragmentExecutionContext(), getOpCtx(), null, tableFunctionConfig);
    tableFunction.setFs(fs);

    try {
      incoming.setAllCount(1);
      incoming.buildSchema();
      outgoing = tableFunction.setup(incoming);

      VarBinaryVector outputDatafileVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing,
        MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.DATA_FILE);

      VarBinaryVector outputSchemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing,
        MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);

      tableFunction.startRow(0);

      assertEquals(1, tableFunction.processRow(0, 5));
      verifyOutput(outputDatafileVector.get(0), outputSchemaVector.get(0), tableSchema, new IcebergPartitionData(PartitionSpec.unpartitioned().partitionType()), IcebergMetadataInformation.IcebergMetadataFileType.ADD_DATAFILE);
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testFooterReadTableFunctionForUnions() throws URISyntaxException, ExecutionSetupException {
    incomingRow.accept(getFullPath("union_bigint_varchar_col2.parquet", FileType.PARQUET), 1507L, currentTime, 0, true);
    FooterReadTableFunction tableFunction = new FooterReadTableFunction(getFragmentExecutionContext(), getOpCtx(), null, getConfig(null, FileType.PARQUET));
    tableFunction.setFs(fs);

    try {
      incoming.setAllCount(2);
      incoming.buildSchema();
      outgoing = tableFunction.setup(incoming);

      VarBinaryVector outputDatafileVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing,
        MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.DATA_FILE);

      VarBinaryVector outputSchemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing,
        MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);

      tableFunction.startRow(0);
      assertEquals(1, tableFunction.processRow(0, 5));
      verifyOutput(outputDatafileVector.get(0), outputSchemaVector.get(0),
        BatchSchema.of(Field.nullable("col1", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), Field.nullable("col2", new ArrowType.Utf8())),
        new IcebergPartitionData(PartitionSpec.unpartitioned().partitionType()), IcebergMetadataInformation.IcebergMetadataFileType.ADD_DATAFILE);
      tableFunction.closeRow();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFooterReadTableFunctionForListOfNull() throws URISyntaxException, ExecutionSetupException {
    incomingRow.accept(getFullPath("list_of_null_in_footer.parquet", FileType.PARQUET), 8416L, currentTime, 0, true);
    /*File schema:
    schema(id:: varchar, count:: int32, creationTime:: timestamp,
    creationTimeOffset:: int32, creationTimeWithOffset:: timestamp,
    lastUpdated:: timestamp, lastUpdatedOffset:: int32,
    lastUpdatedWithOffset:: timestamp, YEAR:: varchar, MONTH:: varchar,
    DAY:: varchar, time:: timestamp, timeOffset:: int32, timeWithOffset:: timestamp,
    history:: list<null>,
    severity:: varchar, source:: varchar,status:: varchar, text:: varchar, type:: varchar)
     */
    FooterReadTableFunction tableFunction = new FooterReadTableFunction(getFragmentExecutionContext(), getOpCtx(), null, getConfig(null, FileType.PARQUET));
    tableFunction.setFs(fs);

    try {
      incoming.setAllCount(20);
      incoming.buildSchema();
      outgoing = tableFunction.setup(incoming);

      VarBinaryVector outputDatafileVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing,
        MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.DATA_FILE);

      VarBinaryVector outputSchemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing,
        MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);

      tableFunction.startRow(0);
      assertEquals(1, tableFunction.processRow(0, 5));
      verifyOutput(outputDatafileVector.get(0), outputSchemaVector.get(0),
        BatchSchema.of(Field.nullable("id", new ArrowType.Utf8()),
          Field.nullable("count", new ArrowType.Int(32, true)),
          Field.nullable("creationTime", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
          Field.nullable("creationTimeOffset", new ArrowType.Int(32, true)),
          Field.nullable("creationTimeWithOffset", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
          Field.nullable("lastUpdated", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
          Field.nullable("lastUpdatedOffset", new ArrowType.Int(32, true)),
          Field.nullable("lastUpdatedWithOffset", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
          Field.nullable("YEAR", new ArrowType.Utf8()),
          Field.nullable("MONTH", new ArrowType.Utf8()),
          Field.nullable("DAY", new ArrowType.Utf8()),
          Field.nullable("time", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
          Field.nullable("timeOffset", new ArrowType.Int(32, true)),
          Field.nullable("timeWithOffset", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
          Field.nullable("severity", new ArrowType.Utf8()),
          Field.nullable("source", new ArrowType.Utf8()),
          Field.nullable("status", new ArrowType.Utf8()),
          Field.nullable("text", new ArrowType.Utf8()),
          Field.nullable("type", new ArrowType.Utf8())),
        new IcebergPartitionData(PartitionSpec.unpartitioned().partitionType()), IcebergMetadataInformation.IcebergMetadataFileType.ADD_DATAFILE);
      tableFunction.closeRow();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testAvroEstimater() throws Exception {
    BatchSchema tableSchema = BatchSchema.of(
      Field.nullablePrimitive("col1", new ArrowType.PrimitiveType.Utf8()),
      Field.nullablePrimitive("col2", new ArrowType.PrimitiveType.Int(32, true)),
      Field.nullablePrimitive("col3", new ArrowType.PrimitiveType.Bool()));
    AvroRowCountEstimater reader = new AvroRowCountEstimater(tableSchema, getOpCtx());
    Path fileRoot = Path.of(Resources.getResource("avro/").toURI());
    String file1 = fileRoot.resolve("avro1.avro").toString();
    Footer footer = reader.getFooter(file1, 300l);
    assertTrue(footer.getRowCount() > 0);
    long file1RowCount = footer.getRowCount();

    String file2 = fileRoot.resolve("avro5").toString();
    footer = reader.getFooter(file2, 340l);
    assertTrue(footer.getRowCount() > file1RowCount);
  }

  private void verifyOutput(byte[] outputDataFileBinary, byte[] outputSchemaBinary, BatchSchema expectedSchema,
                            IcebergPartitionData expectedPartitionData, IcebergMetadataInformation.IcebergMetadataFileType metadataFileType) throws IOException, ClassNotFoundException {
    BatchSchema actualSchema = BatchSchema.deserialize(outputSchemaBinary);
    assertTrue(expectedSchema.equalsTypesWithoutPositions(actualSchema));
    IcebergMetadataInformation icebergMetaInfo = IcebergSerDe.deserializeFromByteArray(outputDataFileBinary);
    assertEquals(metadataFileType, icebergMetaInfo.getIcebergMetadataFileType());
    DataFile actualDataFile = IcebergSerDe.deserializeDataFile(icebergMetaInfo.getIcebergMetadataFileByte());
    int partitions = actualDataFile.partition().size();
    assertEquals(expectedPartitionData.size(), actualDataFile.partition().size());

    for(int i = 0; i < partitions; i++) {
      Object expectedPartition = expectedPartitionData.get(i);
      assertEquals(expectedPartition, actualDataFile.partition().get(i, expectedPartition.getClass()));
    }
  }

  private DataFile getDataFile(byte[] outputDataFileBinary) throws IOException, ClassNotFoundException {
    IcebergMetadataInformation icebergMetaInfo = IcebergSerDe.deserializeFromByteArray(outputDataFileBinary);
    DataFile actualDataFile = IcebergSerDe.deserializeDataFile(icebergMetaInfo.getIcebergMetadataFileByte());
    return actualDataFile;
  }

  private static final Function<BatchSchema, List<SchemaPath>> pathsOfSchemaFields = (schema) -> schema.getFields()
    .stream()
    .map(f -> SchemaPath.getSimplePath(f.getName()))
    .collect(Collectors.toList());

  private static TableFunctionConfig getConfig(BatchSchema tableSchema, FileType fileType) {

    FileConfig fileConfig = new FileConfig();
    fileConfig.setType(fileType);

    FooterReaderTableFunctionContext functionContext = mock(FooterReaderTableFunctionContext.class);
    when(functionContext.getFileType()).thenReturn(fileType);
    when(functionContext.getFullSchema()).thenReturn(MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA);
    when(functionContext.getColumns()).thenReturn(pathsOfSchemaFields.apply(MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA));
    when(functionContext.getFormatSettings()).thenReturn(fileConfig);
    when(functionContext.getTableSchema()).thenReturn(tableSchema);
    when(functionContext.getTablePath()).thenReturn(Arrays.asList(Arrays.asList("dir1", "table1")));

    return new TableFunctionConfig(TableFunctionConfig.FunctionType.FOOTER_READER, true,
      functionContext);
  }

  private FragmentExecutionContext getFragmentExecutionContext() throws ExecutionSetupException {
    FragmentExecutionContext fragmentExecutionContext = mock(FragmentExecutionContext.class);
    when(fragmentExecutionContext.getStoragePlugin(any())).thenReturn(null);
    return fragmentExecutionContext;
  }

  private OperatorContext getOpCtx() {
    SabotContext sabotContext = getSabotContext();
    OperatorContextImpl context =  new OperatorContextImpl(sabotContext.getConfig(), sabotContext.getDremioConfig(), getAllocator(), sabotContext.getOptionManager(), 10, sabotContext.getExpressionSplitCache());

    OpProfileDef prof = new OpProfileDef(1, 1, 1);
    final OperatorStats operatorStats = new OperatorStats(prof, allocator);
    OperatorContextImpl spyContext = spy(context);

    when(spyContext.getStats()).thenReturn(operatorStats);
    return spyContext;
  }

  private String getFullPath(String fileName, FileType fileType) throws URISyntaxException {
    Path tableRoot;
    switch (fileType) {
      case PARQUET:
        tableRoot = Path.of(Resources.getResource("parquet/").toURI());
        return tableRoot.resolve(fileName).toString();
      case AVRO:
        tableRoot = Path.of(Resources.getResource("avro/").toURI());
        return tableRoot.resolve(fileName).toString();
      default:
        throw new UnsupportedOperationException(String.format("Unknown file type - %s", fileType));
    }
  }

  private interface IncomingRowConsumer<A, B, C, D, E> {
    void accept(A path, B size, C mTime, D index, E isAdded);
  }
}


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

import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.extractDataFilePath;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.extractPartitionInfo;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getBatchSchema;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getDatafileWithPartitions;
import static com.dremio.exec.store.iceberg.DataProcessorTestUtil.getVecByName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.DremioManifestReaderUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;

public class TestPathGeneratingDatafileProcessor extends BaseTestQuery {
  private final int startIndex = 0;
  private final int maxOutputCount = 3;

  private VectorContainer outgoing;
  private VarCharVector dataFileVec;
  private VarBinaryVector partitionDataVec;
  private DataFileContentReader datafileProcessor;

  private PartitionSpec partitionSpec;
  private Schema icebergSchema;
  private IcebergPartitionData partitionData;

  @Before
  public void initialisePathGenDatafileProcessor() throws Exception {
    outgoing = getOperatorContext().createOutputVectorContainer();
    OperatorContext operatorContext = getOperatorContext();
    TableFunctionContext tableFunctionContext = mock(TableFunctionContext.class);
    when(tableFunctionContext.getFullSchema()).thenReturn(MetadataRefreshExecConstants.PathGeneratingDataFileProcessor.OUTPUT_SCHEMA.BATCH_SCHEMA);
    outgoing.addSchema(getBatchSchema(DataProcessorTestUtil.DataProcessorType.DATAFILE_PATH_GEN));
    outgoing.buildSchema();
    dataFileVec = (VarCharVector) getVecByName(outgoing, MetadataRefreshExecConstants.PathGeneratingDataFileProcessor.OUTPUT_SCHEMA.DATAFILE_PATH);
    partitionDataVec = (VarBinaryVector) getVecByName(outgoing, MetadataRefreshExecConstants.PathGeneratingDataFileProcessor.OUTPUT_SCHEMA.PARTITION_DATA_PATH);
    datafileProcessor = new DataFileContentReader(operatorContext, tableFunctionContext);
    datafileProcessor.setup(null, outgoing);
    setupPartitionData();
    datafileProcessor.initialise(partitionSpec, 0);
  }

  @After
  public void close() throws Exception {
    outgoing.close();
    datafileProcessor.close();
  }

  @Test
  public void testPathGeneratingDataFileProcessorWithinMaxLimit() throws Exception {
    List<String> dataFilePaths = new ArrayList<>();
    dataFilePaths.add("/path/to/data-0.parquet");
    dataFilePaths.add("/path/to/data-1.parquet");
    dataFilePaths.add("/path/to/data-2.parquet");

    int totalRecords = 0, outputRecords;
    for (String datafilePath : dataFilePaths) {
      DremioManifestReaderUtils.ManifestEntryWrapper<?> entry = new DremioManifestReaderUtils.ManifestEntryWrapper<>(
          getDatafileWithPartitions(datafilePath, 1024L, partitionSpec, partitionData), 0);

      outputRecords = datafileProcessor.processManifestEntry(entry, startIndex + totalRecords, maxOutputCount - totalRecords);
      totalRecords += outputRecords;

      outputRecords = datafileProcessor.processManifestEntry(entry, startIndex + totalRecords, maxOutputCount - totalRecords);
      totalRecords += outputRecords;
      assertEquals(0, outputRecords); // Zero indicates: We are done with current file. ready for processing next
      datafileProcessor.closeManifestEntry();
    }

    String datafile0 = extractDataFilePath(dataFileVec, 0);
    String datafile2 = extractDataFilePath(dataFileVec, 2);

    assertEquals(datafile0, "/path/to/data-0.parquet");
    assertEquals(datafile2, "/path/to/data-2.parquet");
    verifyPartitionInfo(extractPartitionInfo(partitionDataVec, 0), partitionData);
    verifyPartitionInfo(extractPartitionInfo(partitionDataVec, 2), partitionData);
  }

  @Test
  public void testPathGeneratingDataFileProcessorOutOfMaxLimit() throws Exception {
    List<String> datafilePaths = new ArrayList<>();
    datafilePaths.add("/path/to/data-0.parquet");
    datafilePaths.add("/path/to/data-1.parquet");
    datafilePaths.add("/path/to/data-2.parquet");
    datafilePaths.add("/path/to/data-3.parquet");

    int totalRecords = 0, outputRecords;
    for (String datafilePath : datafilePaths) {
      DremioManifestReaderUtils.ManifestEntryWrapper<?> entry = new DremioManifestReaderUtils.ManifestEntryWrapper<>(
          getDatafileWithPartitions(datafilePath, 1024L, partitionSpec, partitionData), 0);

      outputRecords = datafileProcessor.processManifestEntry(entry, startIndex + totalRecords, maxOutputCount - totalRecords);
      totalRecords += outputRecords;
      outputRecords = datafileProcessor.processManifestEntry(entry, startIndex + totalRecords, maxOutputCount - totalRecords);
      totalRecords += outputRecords;
      assertEquals(0, outputRecords); // Zero indicates: We are done with current file. ready for processing next
      datafileProcessor.closeManifestEntry();
    }

    assertEquals(3, totalRecords);
    assertTrue(dataFileVec.isNull(3));
    assertEquals(dataFileVec.isSet(3), 0);
  }

  private void verifyPartitionInfo(IcebergPartitionData actual, IcebergPartitionData expected) {
    assertEquals(actual, expected);
  }

  private OperatorContext getOperatorContext() {
    SabotContext sabotContext = getSabotContext();
    return new OperatorContextImpl(sabotContext.getConfig(), sabotContext.getDremioConfig(), getAllocator(), sabotContext.getOptionManager(), 10, sabotContext.getExpressionSplitCache());
  }

  private void setupPartitionData() {
    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.FLOAT.toField("floatCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitCol"),
        CompleteType.VARCHAR.toField("varCharCol"),
        CompleteType.BIGINT.toField("bigIntCol"),
        CompleteType.VARBINARY.toField("varBinaryCol")
      );

    SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    icebergSchema = schemaConverter.toIcebergSchema(tableSchema);

    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(icebergSchema);

    specBuilder.identity("integerCol");
    specBuilder.identity("floatCol");
    specBuilder.identity("doubleCol");
    specBuilder.identity("bitCol");

    partitionSpec = specBuilder.build();

    partitionData = new IcebergPartitionData(partitionSpec.partitionType());
    partitionData.setInteger(0, 1);
    partitionData.setFloat(1, 1.23f);
    partitionData.setDouble(2, Double.valueOf(1.23f));
    partitionData.setBoolean(3, true);
  }
}

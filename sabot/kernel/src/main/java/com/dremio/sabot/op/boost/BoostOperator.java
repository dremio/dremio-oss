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
package com.dremio.sabot.op.boost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.easy.arrow.ArrowFormatPlugin;
import com.dremio.exec.store.easy.arrow.ArrowFormatPluginConfig;
import com.dremio.exec.store.parquet.GlobalDictionaries;
import com.dremio.exec.store.parquet.ParquetSubScan;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Charsets;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Writes columns of splits in arrow format
 */
public class BoostOperator extends ScanOperator {

  // specifies schema of BoostOperator output
  String SPLIT_COLUMN = "Split";
  String COLUMN_COLUMN = "Column";
  String BOOSTED_COLUMN = "Boosted";

  BatchSchema SCHEMA = BatchSchema.newBuilder()
      .addField(MajorTypeHelper.getFieldForNameAndMajorType(SPLIT_COLUMN, Types.optional(TypeProtos.MinorType.VARCHAR)))
      .addField(MajorTypeHelper.getFieldForNameAndMajorType(COLUMN_COLUMN, Types.optional(TypeProtos.MinorType.VARCHAR)))
      .addField(MajorTypeHelper.getFieldForNameAndMajorType(BOOSTED_COLUMN, Types.optional(TypeProtos.MinorType.BIT)))
      .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
      .build();

  Field SPLIT = SCHEMA.getColumn(0);
  Field COLUMN = SCHEMA.getColumn(1);
  Field BOOSTED = SCHEMA.getColumn(2);

  private final Iterator<SplitAndPartitionInfo> splits;
  private final FileSystemPlugin<?> fsPlugin;
  private SplitAndPartitionInfo currentSplit;
  private final List<ArrowColumnWriter> currentWriters;
  private final ArrowFormatPlugin arrowFormatPlugin;
  private final String tableName;

  private final VectorContainer boostOutputContainer;
  private VarCharVector splitVector;
  private VarCharVector columnVector;
  private BitVector boostedFlagVector;

  public BoostOperator(ParquetSubScan config,
                       OperatorContext context,
                       Iterator<RecordReader> readers,
                       GlobalDictionaries globalDictionaries,
                       FileSystemPlugin<?> fsPlugin) {
    super(config, context, readers, globalDictionaries);
    splits = config.getSplits().iterator();
    this.fsPlugin = fsPlugin;
    currentSplit = splits.next();
    arrowFormatPlugin = (ArrowFormatPlugin) fsPlugin.getFormatPlugin(new ArrowFormatPluginConfig());
    tableName = config.getReferencedTables().stream().flatMap(Collection::stream).collect(Collectors.joining("."));
    currentWriters = new ArrayList<>();
    boostOutputContainer = context.createOutputVectorContainer();
  }

  @Override
  public VectorAccessible setup() throws Exception {
    super.setup();
    // setup writers
    setUpNewWriters();

    splitVector = boostOutputContainer.addOrGet(SPLIT);
    columnVector = boostOutputContainer.addOrGet(COLUMN);
    boostedFlagVector = boostOutputContainer.addOrGet(BOOSTED);
    boostOutputContainer.buildSchema();

    state = State.CAN_PRODUCE;

    if (context.getOptions().getOption(ExecConstants.PARQUET_SCAN_AS_BOOST)) {
      return outgoing;
    }

    return boostOutputContainer;
  }

  @Override
  public int outputData() throws Exception {
    state.is(State.CAN_PRODUCE);
    currentReader.allocate(fieldVectorMap);

    int recordCount;
    while ((recordCount = currentReader.next()) == 0) {
      if (!readers.hasNext()) {
        // We're on the last reader, and it has no (more) rows.
        // no need to close the reader (will be done when closing the operator)
        // but we might as well release any memory that we're holding.
        outgoing.zeroVectors();
        state = State.DONE;
        outgoing.setRecordCount(0);

        if (context.getOptions().getOption(ExecConstants.PARQUET_SCAN_AS_BOOST)) {
          return 0;
        }

        return closeCurrentWritersAndProduceOutputBatch();
      }

      // There are more readers, let's close the previous one and get the next one.
      currentReader.close();
      int boostOutputRecordCount = closeCurrentWritersAndProduceOutputBatch();

      currentReader = readers.next();
      currentSplit = splits.next();
      setupReader(currentReader);
      currentReader.allocate(fieldVectorMap);
      setUpNewWriters();

      if (context.getOptions().getOption(ExecConstants.PARQUET_SCAN_AS_BOOST)) {
        return 0;
      }

      return boostOutputRecordCount; // done with a split; return num of columns boosted
    }
    outgoing.setAllCount(recordCount);
    for (ArrowColumnWriter writer : currentWriters) {
      writer.write(recordCount); // TODO: writes protobufs
    }
    return 0;
  }

  /**
   * creates a writer per column for the new split
   *
   * @throws Exception
   */
  private void setUpNewWriters() throws Exception {
    ParquetProtobuf.ParquetDatasetSplitScanXAttr splitXAttr;
    try {
      splitXAttr = LegacyProtobufSerializer.parseFrom(ParquetProtobuf.ParquetDatasetSplitScanXAttr.PARSER, currentSplit.getDatasetSplitInfo().getExtendedProperty());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Could not deserialize parquet dataset split scan attributes", e);
    }
    String split = Path.of(splitXAttr.getPath()).getName();
    String splitStoreLocation = "/tmp/" + tableName + "/" + split + "/";

    for (Field column : outgoing.getSchema()) {
      String columnName = column.getName();
      String columnStoreLocation = splitStoreLocation + columnName;
      currentWriters.add(new ArrowColumnWriter(splitXAttr.getPath(), columnName, columnStoreLocation));
    }
  }

  /**
   * File stats will be available after closing writers
   * return number of files written by writer
   *
   * @return
   * @throws Exception
   */
  private int closeCurrentWritersAndProduceOutputBatch() throws Exception {
    AutoCloseables.close(currentWriters);

    boostOutputContainer.allocateNew();

    int i = 0;
    for (ArrowColumnWriter writer : currentWriters) {
      splitVector.setSafe(i, writer.splitPath.getBytes(Charsets.UTF_8), 0, writer.splitPath.length());
      columnVector.setSafe(i, writer.column.getBytes(Charsets.UTF_8), 0, writer.column.length());
      boostedFlagVector.setSafe(i, 1);
      i++;
    }
    currentWriters.clear();
    return boostOutputContainer.setAllCount(i);
  }

  private class ArrowColumnWriter implements AutoCloseable {
    private final String column;
    private final String splitPath;
    private final RecordWriter recordWriter;
    private final VectorContainer outputVectorContainer;

    ArrowColumnWriter(String splitPath, String column, String columnStoreLocation) throws IOException {
      this.column = column;
      EasyWriter writerConfig = new EasyWriter(
          config.getProps(),
          null,
          null,
          columnStoreLocation,
          WriterOptions.DEFAULT,
          fsPlugin,
          arrowFormatPlugin);
      recordWriter = arrowFormatPlugin.getRecordWriter(context, writerConfig);

      // container with just one column
      outputVectorContainer = context.createOutputVectorContainer();
      outputVectorContainer.add(fieldVectorMap.get(column.toLowerCase()));
      outputVectorContainer.buildSchema();

      RecordWriter.WriteStatsListener byteCountListener = (b) -> {};
      RecordWriter.OutputEntryListener fileWriteListener = (recordCount, fileSize, path, metadata, partitionNumber, icebergMetadata) -> {};
      recordWriter.setup(outputVectorContainer, fileWriteListener, byteCountListener);

      this.splitPath = splitPath;
    }

    void write(int records) throws IOException {
      outputVectorContainer.setRecordCount(records);
      recordWriter.writeBatch(0, records);
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(recordWriter); // file will be written on closing ArrowRecordWriter
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    AutoCloseables.close(boostOutputContainer, currentWriters);
  }
}

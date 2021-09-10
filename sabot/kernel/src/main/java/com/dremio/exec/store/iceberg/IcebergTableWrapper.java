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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTimeConstants;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.common.HostAffinityComputer;
import com.dremio.exec.store.file.proto.FileProtobuf.FileSystemCachedEntity;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.parquet.ParquetGroupScanUtils;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.IcebergDatasetXAttr;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf.IcebergSchemaField;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ColumnValueCount;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitXAttr;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetXAttr;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

/**
 * Wrapper around iceberg table to build all the metadata details.
 * The current implementation assumes all the datafiles are of parquet type.
 */
public class IcebergTableWrapper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergTableWrapper.class);

  private SabotContext context;
  private FileSystem fs;
  private String rootDir;
  private Table table;
  private Schema schema;
  private BatchSchema batchSchema;
  private List<String> partitionColumns;
  private PartitionChunkListingImpl partitionChunkListing;
  private Map<String, Long> datasetColumnValueCounts;
  private IcebergDatasetXAttr datasetXAttr;
  private BytesOutput readSignature;
  private long recordCount;
  private final IcebergModel icebergModel;

  public IcebergTableWrapper(SabotContext context, FileSystem fs, IcebergModel icebergModel, String rootDir) {
    this.fs = fs;
    this.rootDir = rootDir;
    this.icebergModel = icebergModel;
    this.context = context;
    this.datasetColumnValueCounts = new HashMap<>();
    this.partitionChunkListing = new PartitionChunkListingImpl();
  }

  public IcebergTableInfo getTableInfo() throws IOException {
    // build all info on the first call.
    synchronized (this) {
      if (table == null) {
        // This should be the first step. Else, we can miss updates if there is a race with
        // insert.
        buildReadSignature();
        table = icebergModel.getIcebergTable(
                  icebergModel.getTableIdentifier(rootDir));
        schema = table.schema();
        SchemaConverter schemaConverter = new SchemaConverter(table.name());
        batchSchema = schemaConverter.fromIceberg(table.schema());
        buildPartitionColumns();
        buildPartitionsAndSplits();
        buildDatasetXattr();
      }
    }

    return new IcebergTableInfo() {
      @Override
      public BatchSchema getBatchSchema() {
        return batchSchema;
      }

      @Override
      public long getRecordCount() {
        return recordCount;
      }

      @Override
      public List<String> getPartitionColumns() {
        return partitionColumns;
      }

      @Override
      public PartitionChunkListing getPartitionChunkListing() {
        return partitionChunkListing;
      }

      @Override
      public BytesOutput getExtraInfo() {
        return datasetXAttr::writeTo;
      }

      @Override
      public BytesOutput provideSignature() { return readSignature; }
    };
  }

  // build the names of all the partition columns.
  private void buildPartitionColumns() {
    partitionColumns = table
      .spec()
      .fields()
      .stream()
      .map(PartitionField::sourceId)
      .map(schema::findColumnName) // column name from schema
      .collect(Collectors.toList());
  }

  // build the list of "distinct partition values" and the corresponding dataset splits.
  // TODO: this should be optimised to handle deltas.
  private void buildPartitionsAndSplits() throws IOException {
    PartitionConverter partitionConverter = new PartitionConverter(schema);
    SplitConverter splitConverter = new SplitConverter(context, fs, schema, datasetColumnValueCounts);

    // map of distinct partition values.

    // iterate over all data files to get the partition values and them to the map.
    // TODO ravindra: this iteration requires reading all of the manifest files. This should go via
    // the dremio wrappers.
    for (FileScanTask task : table.newScan().includeColumnStats().planFiles()) {
      List<PartitionValue> partition = partitionConverter.from(task);
      DatasetSplit split = splitConverter.from(task);
      partitionChunkListing.put(partition, split);
      recordCount += task.file().recordCount();
    }
  }

  // populate the PartitionDatasetXAttr using the aggregated per-columnn counts.
  private void buildDatasetXattr() throws IOException {
    IcebergDatasetXAttr.Builder icebergDatasetBuilder = IcebergDatasetXAttr.newBuilder();

    ParquetDatasetXAttr.Builder builder = ParquetDatasetXAttr.newBuilder();
    builder.setSelectionRoot(rootDir);
    for (Map.Entry<String, Long> entry : datasetColumnValueCounts.entrySet()) {
      builder.addColumnValueCountsBuilder()
        .setColumn(entry.getKey())
        .setCount(entry.getValue())
        .build();
    }

    icebergDatasetBuilder.setParquetDatasetXAttr(builder.build());
    Map<String, Integer> schemaNameIDMap = IcebergUtils.getIcebergColumnNameToIDMap(this.schema);
    schemaNameIDMap.forEach((k, v) -> icebergDatasetBuilder.addColumnIds(
      IcebergSchemaField.newBuilder().setSchemaPath(k).setId(v).build()
    ));
    datasetXAttr = icebergDatasetBuilder.build();
  }

  /**
   * Converter from iceberg partition values to dremio partition values.
   */
  static class PartitionConverter {
    private final Schema schema;

    PartitionConverter(Schema schema) {
      this.schema = schema;
    }

    List<PartitionValue> from(FileScanTask task) {
      List<PartitionField> partitionFields = task.spec().fields();
      StructLike filePartition = task.file().partition();

      List<PartitionValue> partitionValues = new ArrayList<>();
      for (int i = 0; i < partitionFields.size(); ++i) {

        Types.NestedField partitionColumnFromSchema = schema.findField(partitionFields.get(i).sourceId());

        if (!partitionFields.get(i).transform().equals(Transforms.identity(partitionColumnFromSchema.type()))) {
          throw UserException.unsupportedError().message("Column values and partition values are not same for [%s] column",
              partitionColumnFromSchema.name()).buildSilently();
        }

        Type resultType = partitionFields.get(i).transform().getResultType(partitionColumnFromSchema.type());

        logger.debug("file {} partitionColumn {} type {} value {}",
          task.file().path(), partitionColumnFromSchema.name(), resultType,
          filePartition.get(i, task.spec().javaClasses()[i]));

        PartitionValue partitionValue = null;
        switch (resultType.typeId()) {
          case INTEGER:
            {
              Integer ival = filePartition.get(i, Integer.class);
              if (ival == null) {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
              } else {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name(), ival);
              }
              break;
            }

          case LONG:
            {
              Long lval = filePartition.get(i, Long.class);
              if (lval == null) {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
              } else {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name(), lval);
              }
              break;
            }

          case DATE:
          {
            Integer ival = filePartition.get(i, Integer.class);
            if (ival == null) {
              partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
            } else {
              long dateInMillis = (long)ival * DateTimeConstants.MILLIS_PER_DAY;
              partitionValue = PartitionValue.of(partitionColumnFromSchema.name(), dateInMillis);
            }
            break;
          }

          case TIME:
          case TIMESTAMP:
            {
              Long tval = filePartition.get(i, Long.class);
              if (tval == null) {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
              } else {
                // convert Timestamp value in Micros to Millis
                // TODO: When we support Timestamp Micros, this conversion has to be based
                // on the type of this field in Dremio's schema
                tval = tval / 1000;
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name(), tval);
              }
              break;
            }

          case FLOAT:
            {
              Float fval = filePartition.get(i, Float.class);
              if (fval == null) {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
              } else {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name(), fval);
              }
              break;
            }

          case DOUBLE:
            {
              Double dval = filePartition.get(i, Double.class);
              if (dval == null) {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
              } else {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name(), dval);
              }
              break;
            }

          case BOOLEAN:
            {
              Boolean bval = filePartition.get(i, Boolean.class);
              if (bval == null) {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
              } else {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name(), bval);
              }
              break;
            }

          case STRING:
            {
              CharSequence cval = filePartition.get(i, CharSequence.class);
              if (cval == null) {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
              } else {
                partitionValue = PartitionValue.of(partitionColumnFromSchema.name(), cval.toString());
              }
              break;
            }

          case DECIMAL:
          {
            BigDecimal dval = filePartition.get(i, BigDecimal.class);
            if (dval == null) {
              partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
            } else {
              partitionValue = PartitionValue.of(partitionColumnFromSchema.name(),
                ByteBuffer.wrap(dval.unscaledValue().toByteArray()));
            }
            break;
          }

          case BINARY:
          {
            ByteBuffer bval = filePartition.get(i, ByteBuffer.class);
            if (bval == null) {
              partitionValue = PartitionValue.of(partitionColumnFromSchema.name());
            } else {
              partitionValue = PartitionValue.of(partitionColumnFromSchema.name(), bval);
            }
            break;
          }

          default:
            throw new UnsupportedOperationException(resultType + " is not supported");
        }
        partitionValues.add(partitionValue);
      }
      return partitionValues;
    }
  }

  /**
   * Convert an iceberg split into a DatasetSplit.
   */
  private static class SplitConverter {
    private final Schema schema;
    private final FileSystem fs;
    private final Map<String, Long> datasetColumnValueCounts;
    private final Set<HostAndPort> activeHostMap = Sets.newHashSet();
    private final Set<HostAndPort> activeHostPortMap = Sets.newHashSet();

    SplitConverter(SabotContext context, FileSystem fs, Schema schema,
      Map<String, Long> datasetColumnValueCounts) {
      this.schema = schema;
      this.fs = fs;
      this.datasetColumnValueCounts = datasetColumnValueCounts;

      for (NodeEndpoint endpoint : context.getExecutors()) {
        activeHostMap.add(HostAndPort.fromHost(endpoint.getAddress()));
        activeHostPortMap.add(HostAndPort.fromParts(endpoint.getAddress(), endpoint.getFabricPort()));
      }
    }

    DatasetSplit from(FileScanTask task) throws IOException {
      // TODO ravindra: iceberg does not track counts at a row-group level. We should fallback to
      // an alternate codepath for this.
      DataFile dataFile = task.file();
      if (dataFile.splitOffsets() != null && dataFile.splitOffsets().size() > 1) {
        throw new UnsupportedOperationException("iceberg does not support multiple row groups yet");
      }
      String pathString = dataFile.path().toString();
      FileAttributes fileAttributes = fs.getFileAttributes(Path.of(pathString));

      // Get the per-column value counts. The counts may not be present.
      List<ColumnValueCount> columnValueCounts = Lists.newArrayList();
      if (dataFile.valueCounts() != null) {
        for (Map.Entry<Integer, Long> entry : dataFile.valueCounts().entrySet()) {
          String columnName = schema.findColumnName(entry.getKey());
          Types.NestedField field = schema.findField(entry.getKey());
          if (field == null || columnName  == null) {
            // we are not updating counts for list elements.
            continue;
          }

          Long totalCount = entry.getValue();
          Long nullValueCount = dataFile.nullValueCounts().get(entry.getKey());
          if (totalCount == null || nullValueCount == null) {
            continue;
          }

          long nonNullValueCount = totalCount - nullValueCount;
          columnValueCounts.add(
              ColumnValueCount.newBuilder()
                  .setColumn(columnName)
                  .setCount(nonNullValueCount)
                  .build());
          // aggregate into the dataset level column-value counts.
          datasetColumnValueCounts.merge(
              columnName, nonNullValueCount, (x, y) -> y + nonNullValueCount);
        }
      }

      // Create the split-level xattr with details about the column counts.
      // TODO: not populating column bounds here since they are not used by dremio planner.
      ParquetDatasetSplitXAttr splitExtended =
          ParquetDatasetSplitXAttr.newBuilder()
              .setPath(pathString)
              .setStart(task.start())
              .setRowGroupIndex(0)
              .setUpdateKey(
                  FileSystemCachedEntity.newBuilder()
                      .setPath(pathString)
                      .setLastModificationTime(fileAttributes.lastModifiedTime().toMillis())
                      .setLength(fileAttributes.size()))
              .addAllColumnValueCounts(columnValueCounts)
              .setLength(task.length())
              .build();

      // build the host affinity details for the split.
      Iterable<FileBlockLocation> blockLocations = fs.getFileBlockLocations(fileAttributes, task.start(), task.length());
      Map<HostAndPort, Float> affinities = HostAffinityComputer.computeAffinitiesForSplit(
        task.start(), task.length(), blockLocations, fs.preserveBlockLocationsOrder());
      List<DatasetSplitAffinity> splitAffinities = new ArrayList<>();
      for (ObjectLongCursor<HostAndPort> item :
        ParquetGroupScanUtils.buildEndpointByteMap(activeHostMap,
          activeHostPortMap, affinities, task.length())) {
        splitAffinities.add(DatasetSplitAffinity.of(item.key.toString(), item.value));
      }

      return DatasetSplit.of(
          splitAffinities, task.length(), task.file().recordCount(), splitExtended::writeTo);
    }
  }

  // Use the mtime on the metadata directory as the read signature.
  private void buildReadSignature() throws IOException {
    Path metaDir = Path.of(rootDir).resolve(IcebergFormatMatcher.METADATA_DIR_NAME);
    if (!fs.exists(metaDir) || !fs.isDirectory(metaDir)) {
      throw new IllegalStateException("missing metadata dir for iceberg table");
    }

    final FileAttributes attributes = fs.getFileAttributes(metaDir);
    final FileSystemCachedEntity cachedEntity = FileSystemCachedEntity
      .newBuilder()
      .setPath(metaDir.toString())
      .setLastModificationTime(attributes.lastModifiedTime().toMillis())
      .setLength(attributes.size())
      .build();

    readSignature = FileUpdateKey
      .newBuilder()
      .addCachedEntities(cachedEntity)
      .build()::writeTo;
  }

}

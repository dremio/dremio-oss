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
package com.dremio.exec.store.parquet;

import static com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.UPDATE_COLUMN;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.physical.base.ScanStats.GroupScanProperty;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SearchableBatchSchema;
import com.dremio.exec.store.dfs.CompleteFileWork.FileWorkImpl;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.parquet.Metadata.ColumnMetadata;
import com.dremio.exec.store.parquet.Metadata.ParquetFileMetadata;
import com.dremio.exec.store.parquet.Metadata.ParquetTableMetadata;
import com.dremio.exec.store.parquet.Metadata.RowGroupMetadata;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.exec.store.schedule.EndpointByteMap;
import com.dremio.exec.store.schedule.EndpointByteMapImpl;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.options.OptionManager;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

public class ParquetGroupScanUtils {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetGroupScanUtils.class);

  private final FileSystemPlugin<?> plugin;
  private final ParquetFormatPlugin formatPlugin;
  private String selectionRoot;
  private List<SchemaPath> columns;
  private List<RowGroupInfo> rowGroupInfos;
  private List<ParquetFilterCondition> conditions;
  private final List<FileAttributes> entries;
  private final FileSystem fs;
  private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryColumns;
  private ParquetTableMetadata parquetTableMetadata = null;

  /*
   * total number of non-null value for each column in each parquet file.
   */
  private Map<SchemaPath, Long> columnValueCounts;
  // Map from file names to maps of column name to partition value mappings
  private Map<FileAttributes, Map<SchemaPath, Object>> partitionValueMap = Maps.newHashMap();
  // Preserve order of insertion, need it to prune the map later if it goes above threshold.
  private Map<SchemaPath, MajorType> columnTypeMap = Maps.newLinkedHashMap();

  private final SearchableBatchSchema schema;
  private final OptionManager optionManager;

  /**
   * total number of rows (obtained from parquet footer)
   */
  private long rowCount;

  public ParquetGroupScanUtils(
    String userName,
    FileSelection selection,
    FileSystemPlugin<?> plugin,
    ParquetFormatPlugin formatPlugin,
    String selectionRoot,
    List<SchemaPath> columns,
    BatchSchema schema,
    Map<String, GlobalDictionaryFieldInfo> globalDictionaryColumns,
    List<ParquetFilterCondition> conditions, OptionManager optionManager)
      throws IOException {
    this.schema = SearchableBatchSchema.of(schema);
    this.formatPlugin = formatPlugin;
    this.conditions = conditions;
    this.columns = columns;
    this.plugin = plugin;
    this.fs = plugin.createFS(userName, null, true);
    this.selectionRoot = selectionRoot;
    this.entries = selection.getFileAttributesList();

    this.globalDictionaryColumns = (globalDictionaryColumns == null)? Collections.<String, GlobalDictionaryFieldInfo>emptyMap() : globalDictionaryColumns;
    this.optionManager = optionManager;
    init();
  }

  public List<FileAttributes> getEntries() {
    return entries;
  }

  public ParquetFormatConfig getFormatConfig() {
    return this.formatPlugin.getConfig();
  }

  public FileSystemPlugin<?> getPlugin() {
    return plugin;
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public Map<FileAttributes, Map<SchemaPath, Object>> getPartitionValueMap() {
    return partitionValueMap;
  }

  public Map<SchemaPath, MajorType> getColumnTypeMap() {
    return columnTypeMap;
  }

  public Map<String, GlobalDictionaryFieldInfo> getGlobalDictionaryColumns() {
    return globalDictionaryColumns;
  }

  private boolean isFieldTypeUnion(SchemaPath schemaPath) {
    // if field present in schema is of union type, then don't use it as partitioned column
    Optional<Field> field = this.schema.findFieldIgnoreCase(schemaPath.getRootSegment().getNameSegment().getPath());
    return field.isPresent() && CompleteType.fromField(field.get()).isUnion();
  }

  /**
   * When reading the very first footer, any column is a potential partition column. So for the first footer, we check
   * every column to see if it is single valued, and if so, add it to the list of potential partition columns. For the
   * remaining footers, we will not find any new partition columns, but we may discover that what was previously a
   * potential partition column now no longer qualifies, so it needs to be removed from the list.
   * @return whether column is a potential partition column
   */
  private boolean checkForPartitionColumn(ParquetFileMetadata fileMetadata, int rowGroupIdx,
      ColumnMetadata columnMetadata, boolean first, long rowCount) {
    SchemaPath schemaPath = SchemaPath.getCompoundPath(columnMetadata.getName());
    if (schemaPath.getAsUnescapedPath().equals(UPDATE_COLUMN)) {
      return true;
    }

    final PrimitiveTypeName primitiveType = fileMetadata.getPrimitiveType(columnMetadata.getName());
    final OriginalType originalType = fileMetadata.getOriginalType(columnMetadata.getName());

    if (first) {
      if (hasSingleValue(columnMetadata, rowCount) && !isFieldTypeUnion(schemaPath)) {
        logger.debug("New partition {} added to list, table {}, file {}, rowgroup index {}",
            schemaPath, selectionRoot, fileMetadata.getPathString(), rowGroupIdx);
        columnTypeMap.put(schemaPath, getType(primitiveType, originalType));
        return true;
      } else {
        logger.debug("Column {} is determined to be non-partition column, table {}, file {}, rowgroup index {}",
            schemaPath, selectionRoot, fileMetadata.getPathString(), rowGroupIdx);
        return false;
      }
    } else {
      if (!columnTypeMap.keySet().contains(schemaPath)) {
        return false;
      } else {
        if (!hasSingleValue(columnMetadata, rowCount)) {
          logger.debug("Column {} is demoted to non-partition column due to non-unique values in new file/rowgroup, " +
                  "table {}, file {}, rowgroup index {}",
              schemaPath, selectionRoot, fileMetadata.getPathString(), rowGroupIdx);
          columnTypeMap.remove(schemaPath);
          return false;
        }
        final MajorType newType = getType(primitiveType, originalType);
        final MajorType existingType = columnTypeMap.get(schemaPath);
        if (!newType.equals(existingType)) {
          logger.debug("Column {} is demoted to non-partition column due to type change: existing: {}, new: {}, " +
                  "table {}, file {}, rowgroup index {}",
              schemaPath, existingType, newType, selectionRoot, fileMetadata.getPathString(), rowGroupIdx);
          columnTypeMap.remove(schemaPath);
          return false;
        }
      }
    }
    return true;
  }

  private MajorType getType(PrimitiveTypeName type, OriginalType originalType) {
    if (originalType != null) {
      switch (originalType) {
        case DECIMAL:
          return Types.optional(MinorType.DECIMAL);
        case DATE:
          return Types.optional(MinorType.DATE);
        case TIME_MILLIS:
          return Types.optional(MinorType.TIME);
        case TIMESTAMP_MILLIS:
          return Types.optional(MinorType.TIMESTAMP);
        case UTF8:
          return Types.optional(MinorType.VARCHAR);
        case UINT_8:
          return Types.optional(MinorType.INT);
        case UINT_16:
          return Types.optional(MinorType.INT);
        case UINT_32:
          return Types.optional(MinorType.BIGINT);
        case UINT_64:
          return Types.optional(MinorType.BIGINT);
        case INT_8:
          return Types.optional(MinorType.INT);
        case INT_16:
          return Types.optional(MinorType.INT);
      }
    }

    switch (type) {
      case BOOLEAN:
        return Types.optional(MinorType.BIT);
      case INT32:
        return Types.optional(MinorType.INT);
      case INT64:
        return Types.optional(MinorType.BIGINT);
      case FLOAT:
        return Types.optional(MinorType.FLOAT4);
      case DOUBLE:
        return Types.optional(MinorType.FLOAT8);
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return Types.optional(MinorType.VARBINARY);
      case INT96:
        return Types.optional(MinorType.TIMESTAMP);
      default:
        // Should never hit this
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }

  private boolean hasSingleValue(ColumnMetadata columnChunkMetaData, long rowCount) {
    // Return try if min == max and there are no null values or all of them are null values.
    return (columnChunkMetaData != null) &&
      ((columnChunkMetaData.hasSingleValue() && (columnChunkMetaData.getNulls() == null || columnChunkMetaData.getNulls() == 0) ||
        (columnChunkMetaData.getNulls() != null && rowCount == columnChunkMetaData.getNulls())));

  }

  public static class RowGroupInfo extends FileWorkImpl implements CompleteWork {

    private EndpointByteMap byteMap;
    private int rowGroupIndex;
    private long rowCount;  // rowCount = -1 indicates to include all rows.
    private List<EndpointAffinity> affinities;
    private Map<SchemaPath, Long> columnValueCounts;

    public RowGroupInfo(FileAttributes fileAttributes, long start, long length, int rowGroupIndex, long rowCount, Map<SchemaPath, Long> columnValueCounts) {
      super(start, length, fileAttributes);
      this.rowGroupIndex = rowGroupIndex;
      this.rowCount = rowCount;
      this.columnValueCounts = columnValueCounts == null? Collections.<SchemaPath, Long>emptyMap() : columnValueCounts;
    }

    public int getRowGroupIndex() {
      return this.rowGroupIndex;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return Long.compare(getTotalBytes(), o.getTotalBytes());
    }

    @Override
    public List<EndpointAffinity> getAffinity() {
      return affinities;
    }

    @Override
    public long getTotalBytes() {
      return this.getLength();
    }

    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    public Map<SchemaPath, Long> getColumnValueCounts() {
      return columnValueCounts;
    }

    public void setEndpointByteMap(EndpointByteMap byteMap) {
      this.byteMap = byteMap;
      this.affinities = Lists.newArrayList();
      final Iterator<ObjectLongCursor<HostAndPort>> hostPortIterator = byteMap.iterator();
      while (hostPortIterator.hasNext()) {
        ObjectLongCursor<HostAndPort> nodeEndPoint = hostPortIterator.next();
        affinities.add(EndpointAffinity.fromHostAndPort(nodeEndPoint.key, nodeEndPoint.value));
      }
    }

    public long getRowCount() {
      return rowCount;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      RowGroupInfo that = (RowGroupInfo) o;
      return rowGroupIndex == that.rowGroupIndex &&
          rowCount == that.rowCount;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(super.hashCode(), rowGroupIndex, rowCount);
    }
  }

  private void init() throws IOException {
    final Stopwatch watch = Stopwatch.createStarted();
    columnTypeMap.put(SchemaPath.getSimplePath(UPDATE_COLUMN), Types.optional(MinorType.BIGINT));
    long maxFooterLength = plugin.getContext().getOptionManager().getOption(ExecConstants.PARQUET_MAX_FOOTER_LEN_VALIDATOR);
    // TODO: do we need this code path?
    final long maxSplits = optionManager.getOption(Metadata.DFS_MAX_SPLITS);
    if (entries.size() == 1) {
      parquetTableMetadata = Metadata.getParquetTableMetadata(entries.get(0), fs, formatPlugin.getConfig(), maxFooterLength, maxSplits);
    } else {
      parquetTableMetadata = Metadata.getParquetTableMetadata(entries, fs, formatPlugin.getConfig(), maxFooterLength, maxSplits);
    }

    Set<HostAndPort> hostEndpointMap = Sets.newHashSet();
    Set<HostAndPort> hostPortEndpointMap = Sets.newHashSet();
    for (NodeEndpoint endpoint : plugin.getContext().getExecutors()) {
      hostEndpointMap.add(HostAndPort.fromHost(endpoint.getAddress()));
      hostPortEndpointMap.add(HostAndPort.fromParts(endpoint.getAddress(), endpoint.getFabricPort()));
    }

    rowGroupInfos = Lists.newArrayList();
    for (ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      int rgIndex = 0;
      for (RowGroupMetadata rg : file.getRowGroups()) {
        // non null value counts for column
        long rowCount = rg.getRowCount();
        Map<SchemaPath, Long> rowGroupColumnValueCounts = Maps.newHashMap();
        for (ColumnMetadata column : rg.getColumns()) {
          SchemaPath schemaPath = SchemaPath.getCompoundPath(column.getName());
          if (column.getNulls() != null) {
            rowGroupColumnValueCounts.put(schemaPath, rowCount - column.getNulls());
          }
        }
        RowGroupInfo rowGroupInfo = new RowGroupInfo(file.getFileAttributes(), rg.getStart(), rg.getLength(), rgIndex, rg.getRowCount(), rowGroupColumnValueCounts);

        EndpointByteMap endpointByteMap = buildEndpointByteMap(hostEndpointMap, hostPortEndpointMap, rg.getHostAffinity(),
          rg.getLength());
        rowGroupInfo.setEndpointByteMap(endpointByteMap);
        rgIndex++;
        rowGroupInfos.add(rowGroupInfo);
      }
    }

    columnValueCounts = Maps.newHashMap();
    this.rowCount = 0;
    boolean first = true;
    for (ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      int rowGroupIdx = 0;
      for (RowGroupMetadata rowGroup : file.getRowGroups()) {
        long rowCount = rowGroup.getRowCount();
        for (ColumnMetadata column : rowGroup.getColumns()) {
          SchemaPath schemaPath = SchemaPath.getCompoundPath(column.getName());
          Long previousCount = columnValueCounts.get(schemaPath);
          if (previousCount != null) {
            if (previousCount != GroupScan.NO_COLUMN_STATS) {
              if (column.getNulls() != null) {
                long newCount = rowCount - column.getNulls();
                // Update the count only when there are any non-zero non-nulls
                if (newCount != 0) {
                  columnValueCounts.put(schemaPath, columnValueCounts.get(schemaPath) + newCount);
                }
              } else {
                // Set to no column stats since at-least one row group exists where
                // stats are in-correct.
                columnValueCounts.put(schemaPath, GroupScan.NO_COLUMN_STATS);
              }
            }
          } else {
            if (column.getNulls() != null) {
              Long newCount = rowCount - column.getNulls();
              columnValueCounts.put(schemaPath, newCount);
            } else {
              columnValueCounts.put(schemaPath, GroupScan.NO_COLUMN_STATS);
            }
          }
          boolean partitionColumn = checkForPartitionColumn(file, rowGroupIdx, column, first, rowCount);
          if (partitionColumn) {
            Map<SchemaPath, Object> map = partitionValueMap.get(file.getFileAttributes());
            if (map == null) {
              map = Maps.newHashMap();
              partitionValueMap.put(file.getFileAttributes(), map);

            }
            Object value = map.get(schemaPath);
            Object currentValue;
            // If all the values are null, then consider the partition value as null, otherwise get the partition value
            // from max.
            if (column.getNulls() != null && column.getNulls() == rowCount) {
              currentValue = null;
            } else {
              currentValue = column.getMaxValue();
            }

            if (rowGroupIdx > 0) {
              // If this is not the first rowgroup in the file, make sure it matches the value in previous rowgroup(s)
              if (!Objects.equal(value, currentValue)) {
                logger.debug("Column {} is demoted to non-partition column due to different values across rowgroups" +
                    " in same file, existing value: {}, new value: {}, table {}, file {}, rowgroup index {}",
                    schemaPath, value, currentValue, selectionRoot, file.getPathString(), rowGroupIdx);
                columnTypeMap.remove(schemaPath);
              }
            } else {
              // as this is the first rowgroup in file, just insert it into map.
              map.put(schemaPath, currentValue);
            }
          } else {
            columnTypeMap.remove(schemaPath);
          }
        }
        this.rowCount += rowGroup.getRowCount();
        first = false;
        rowGroupIdx++;
      }

      if (file.getRowGroups().size() == 0) {
        continue;
      }

      Map<SchemaPath, Object> map = partitionValueMap.get(file.getFileAttributes());
      if (map == null) {
        map = Maps.newHashMap();
        partitionValueMap.put(file.getFileAttributes(), map);
      }
      map.put(SchemaPath.getSimplePath(UPDATE_COLUMN), file.getFileAttributes().lastModifiedTime().toMillis());
    }

    eliminateSomePartitionColumns();

    logger.debug("Table: {}, partition columns {}", selectionRoot, columnTypeMap.keySet());
    logger.debug("Took {} ms to gather Parquet table metadata.", watch.elapsed(TimeUnit.MILLISECONDS));
  }

  public static EndpointByteMap buildEndpointByteMap(
    Set<HostAndPort> activeHostMap, Set<HostAndPort> activeHostPortMap,
    Map<com.google.common.net.HostAndPort, Float> affinities, long totalLength) {

    EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
    for (HostAndPort host : affinities.keySet()) {
      HostAndPort endpoint = null;
      if (!host.hasPort()) {
        if (activeHostMap.contains(host)) {
          endpoint = host;
        }
      } else {
        // multi executor deployment and affinity provider is sensitive to the port
        // picking the map late as it allows a source that contains files in HDFS and S3
        if (activeHostPortMap.contains(host)) {
          endpoint = host;
        }
      }

      if (endpoint != null) {
        endpointByteMap.add(endpoint, (long) (affinities.get(host) * totalLength));
      }
    }
    return endpointByteMap;
  }

  private void eliminateSomePartitionColumns() {

    // DX-14064: don't consider columns that are all null partition columns.
    if (optionManager.getOption(ExecConstants.PARQUET_ELIMINATE_NULL_PARTITIONS)) {
      // filter out only those columns who we know have zero count i.e. all the row groups
      // have stats for this column and they are all null.
      columnTypeMap = columnTypeMap.entrySet()
        .stream()
        .filter(e -> e.getKey().getAsUnescapedPath().equals(IncrementalUpdateUtils.UPDATE_COLUMN)
          || columnValueCounts.get(e.getKey()) != 0)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1,
                 LinkedHashMap::new));
    }

    // DX-14064: Limit the total number of partition columns to a defined threshold.
    final int maxPartitionColumns = (int) optionManager.getOption(ExecConstants.PARQUET_MAX_PARTITION_COLUMNS_VALIDATOR);
    if (columnTypeMap.size() > maxPartitionColumns) {
      logger.debug("Table: {} having partitioned column count {} which is more than the " +
        "threshold {}, pruning.", selectionRoot, columnTypeMap.size(), maxPartitionColumns);
      Map<SchemaPath, MajorType> prunedColumnTypeMap = Maps.newLinkedHashMap();
      int i = 0;
      for(Map.Entry<SchemaPath, MajorType> columnTypeMapEntry : columnTypeMap.entrySet()) {
        if (i == maxPartitionColumns) {
          break;
        }
        prunedColumnTypeMap.put(columnTypeMapEntry.getKey(), columnTypeMapEntry.getValue());
        i++;
      }
      // handle case where partition identification is turned off.
      // this is needed to correctly handle incremental reflection refresh.
      if (prunedColumnTypeMap.size() == 0 ) {
        prunedColumnTypeMap.put(SchemaPath.getSimplePath(UPDATE_COLUMN), Types.optional(MinorType.BIGINT));
      }
      columnTypeMap = prunedColumnTypeMap;
    }
  }

  public int getMaxParallelizationWidth() {
    return rowGroupInfos.size();
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public Map<SchemaPath, Long> getColumnValueCounts() {
    return columnValueCounts;
  }

  public long getRowCount() {
    return rowCount;
  }

  public ScanStats getScanStats() {
    int columnCount = columns == null ? 20 : columns.size();
    if (GroupScan.ALL_COLUMNS.equals(columns)) {
      columnCount = schema.getFieldCount();
    }
    int sortFactor = getSortFactor();
    if(hasConditions()){
      long estRowCount = (long) (this.rowCount * 0.15d);
      return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, 1 / sortFactor, estRowCount * columnCount / sortFactor);
    } else {
      return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, rowCount, 1 / sortFactor, rowCount * columnCount / sortFactor);
    }
  }

  @JsonIgnore
  public ScanCostFactor getScanCostFactor() {
    return ScanCostFactor.of(ScanCostFactor.PARQUET.getFactor() / getSortFactor());
  }

  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "ParquetGroupScanUtils [entries=" + entries
        + ", selectionRoot=" + selectionRoot
        + ", numFiles=" + getEntries().size()
        + ", conditions=" + conditions
        + ", columns=" + columns + "]";
  }

  @JsonIgnore
  public List<RowGroupInfo> getRowGroupInfos() {
    return rowGroupInfos;
  }

  public boolean hasConditions(){
    return conditions != null && !conditions.isEmpty();
  }

  public int getSortFactor() {
    if (!hasConditions()) {
      return 1;
    }
    ParquetFilterCondition condition = Iterables.getFirst(conditions, null);
    int sortIndex = condition.getSort();
    if (sortIndex < 0) {
      return 1;
    }
    // somewhat arbitrary formula, but the goal is for the factor to be lower if the sort column is secondary than if it's primary
    // e.g. table is sorted on a, b, then a filter on a should be cheaper than a filter on b
    return Math.max(1, 16 / (sortIndex + 1));
  }

  public List<ParquetFilterCondition> getConditions() {
    return conditions;
  }

  public List<SchemaPath> getPartitionColumns() {
    List<SchemaPath> list = Lists.newArrayList();
    return ImmutableList.<SchemaPath>builder().addAll(list).addAll(columnTypeMap.keySet()).build();
  }

}

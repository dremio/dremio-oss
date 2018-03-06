/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.util.ImpersonationUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Metadata {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata.class);

  private final FileSystem fs;
  private final ParquetFormatConfig formatConfig;

  /**
   * Get the parquet metadata for the parquet files in the given directory, including those in subdirectories
   *
   * @param fs
   * @return
   * @throws IOException
   */
  public static ParquetTableMetadata getParquetTableMetadata(FileStatus status, FileSystem fs,
      ParquetFormatConfig formatConfig, Configuration fsConf) throws IOException {
    Metadata metadata = new Metadata(formatConfig, fsConf);
    return metadata.getParquetTableMetadata(ImmutableList.of(status));
  }

  /**
   * Get the parquet metadata for a list of parquet files
   *
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  public static ParquetTableMetadata getParquetTableMetadata(
    List<FileStatus> fileStatuses, ParquetFormatConfig formatConfig, Configuration fsConf) throws IOException {
    Metadata metadata = new Metadata(formatConfig, fsConf);
    return metadata.getParquetTableMetadata(fileStatuses);
  }

  private Metadata(ParquetFormatConfig formatConfig, Configuration fsConf) {
    this.fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fsConf);
    this.formatConfig = formatConfig;
  }

  /**
   * Get the parquet metadata for a list of parquet files
   *
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  private ParquetTableMetadata getParquetTableMetadata(List<FileStatus> fileStatuses)
      throws IOException {
    List<ParquetFileMetadata> fileMetadataList = getParquetFileMetadata(fileStatuses);
    return new ParquetTableMetadata(fileMetadataList);
  }

  /**
   * Get a list of file metadata for a list of parquet files
   *
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  private List<ParquetFileMetadata> getParquetFileMetadata(List<FileStatus> fileStatuses) throws IOException {
    List<TimedRunnable<ParquetFileMetadata>> gatherers = Lists.newArrayList();
    for (FileStatus file : fileStatuses) {
      gatherers.add(new MetadataGatherer(file));
    }

    List<ParquetFileMetadata> metaDataList = Lists.newArrayList();
    metaDataList.addAll(TimedRunnable.run("Fetch parquet metadata", logger, gatherers, 16));
    return metaDataList;
  }

  /**
   * TimedRunnable that reads the footer from parquet and collects file metadata
   */
  private class MetadataGatherer extends TimedRunnable<ParquetFileMetadata> {

    private FileStatus fileStatus;

    public MetadataGatherer(FileStatus fileStatus) {
      this.fileStatus = fileStatus;
    }

    @Override
    protected ParquetFileMetadata runInner() throws Exception {
      final UserGroupInformation processUGI = ImpersonationUtil.getProcessUserUGI();
      return processUGI.doAs(new PrivilegedExceptionAction<ParquetFileMetadata>() {
        @Override
        public ParquetFileMetadata run() throws Exception {
          return getParquetFileMetadata(fileStatus);
        }
      });
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      if (e instanceof IOException) {
        return (IOException) e;
      } else {
        return new IOException(e);
      }
    }
  }

  private OriginalType getOriginalType(Type type, String[] path, int depth) {
    if (type.isPrimitive()) {
      return type.getOriginalType();
    }
    Type t = ((GroupType) type).getType(path[depth]);
    return getOriginalType(t, path, depth + 1);
  }

  private ParquetFileMetadata getParquetFileMetadata(FileStatus file) throws IOException {
    final ParquetMetadata metadata;

    metadata = SingletonParquetFooterCache.readFooter(fs, file, ParquetMetadataConverter.NO_FILTER);

    MessageType schema = metadata.getFileMetaData().getSchema();

    Map<SchemaPath, OriginalType> originalTypeMap = Maps.newHashMap();
    schema.getPaths();
    for (String[] path : schema.getPaths()) {
      originalTypeMap.put(SchemaPath.getCompoundPath(path), getOriginalType(schema, path, 0));
    }

    List<RowGroupMetadata> rowGroupMetadataList = Lists.newArrayList();

    ArrayList<SchemaPath> ALL_COLS = new ArrayList<>();
    ALL_COLS.add(AbstractRecordReader.STAR_COLUMN);
    boolean autoCorrectCorruptDates = formatConfig.autoCorrectCorruptDates;
    ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(metadata, ALL_COLS, autoCorrectCorruptDates);
    if(logger.isDebugEnabled()){
      logger.debug(containsCorruptDates.toString());
    }
    final Map<ColumnTypeMetadata.Key, ColumnTypeMetadata> columnTypeInfo = Maps.newHashMap();
    int rowGroupIdx = 0;
    for (BlockMetaData rowGroup : metadata.getBlocks()) {
      List<ColumnMetadata> columnMetadataList = Lists.newArrayList();
      long length = 0;
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        ColumnMetadata columnMetadata;

        boolean statsAvailable = (col.getStatistics() != null && !col.getStatistics().isEmpty());

        Statistics<?> stats = col.getStatistics();
        String[] columnName = col.getPath().toArray();
        SchemaPath columnSchemaName = SchemaPath.getCompoundPath(columnName);
        ColumnTypeMetadata columnTypeMetadata =
            new ColumnTypeMetadata(columnName, col.getType(), originalTypeMap.get(columnSchemaName));

        columnTypeInfo.put(new ColumnTypeMetadata.Key(columnTypeMetadata.name), columnTypeMetadata);
        if (statsAvailable) {
          // Write stats only if minVal==maxVal. Also, we then store only maxVal
          Object mxValue = null;
          if (stats.genericGetMax() != null && stats.genericGetMin() != null &&
              stats.genericGetMax().equals(stats.genericGetMin())) {
            mxValue = stats.genericGetMax();
            if (containsCorruptDates == ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_CORRUPTION
                && columnTypeMetadata.originalType == OriginalType.DATE) {
              mxValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) mxValue);
            }
          }
          columnMetadata =
              new ColumnMetadata(columnTypeMetadata.name, mxValue, stats.getNumNulls());
        } else {
          // log it under trace to avoid lot of log entries.
          logger.trace("Stats are not available for column {}, rowGroupIdx {}, file {}",
              columnSchemaName, rowGroupIdx, file.getPath());
          columnMetadata = new ColumnMetadata(columnTypeMetadata.name,null, null);
        }
        columnMetadataList.add(columnMetadata);
        length += col.getTotalSize();
      }

      RowGroupMetadata rowGroupMeta =
          new RowGroupMetadata(rowGroup.getStartingPos(), length, rowGroup.getRowCount(),
              getHostAffinity(file, rowGroup.getStartingPos(), length), columnMetadataList);

      rowGroupMetadataList.add(rowGroupMeta);
      rowGroupIdx++;
    }

    return new ParquetFileMetadata(file, file.getLen(), rowGroupMetadataList, columnTypeInfo);
  }

  /**
   * Get the host affinity for a row group
   *
   * @param fileStatus the parquet file
   * @param start      the start of the row group
   * @param length     the length of the row group
   * @return
   * @throws IOException
   */
  private Map<String, Float> getHostAffinity(FileStatus fileStatus, long start, long length)
      throws IOException {
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, start, length);
    Map<String, Float> hostAffinityMap = Maps.newHashMap();
    for (BlockLocation blockLocation : blockLocations) {
      for (String host : blockLocation.getHosts()) {
        Float currentAffinity = hostAffinityMap.get(host);
        float blockStart = blockLocation.getOffset();
        float blockEnd = blockStart + blockLocation.getLength();
        float rowGroupEnd = start + length;
        Float newAffinity = (blockLocation.getLength() - (blockStart < start ? start - blockStart : 0) -
            (blockEnd > rowGroupEnd ? blockEnd - rowGroupEnd : 0)) / length;
        if (currentAffinity != null) {
          hostAffinityMap.put(host, currentAffinity + newAffinity);
        } else {
          hostAffinityMap.put(host, newAffinity);
        }
      }
    }
    return hostAffinityMap;
  }


  /**
   * Struct which contains the metadata for an entire parquet directory structure
   */
  public static class ParquetTableMetadata {
    private final List<ParquetFileMetadata> files;

    private ParquetTableMetadata(List<ParquetFileMetadata> files) {
      this.files = files;
    }

    public List<? extends ParquetFileMetadata> getFiles() {
      return files;
    }
  }


  /**
   * Struct which contains the metadata for a single parquet file
   */
  public static class ParquetFileMetadata {
    private final FileStatus status;
    private final String path;
    private final Long length;
    private final List<RowGroupMetadata> rowGroups;
    private final Map<ColumnTypeMetadata.Key, ColumnTypeMetadata> columnTypeInfo;

    public ParquetFileMetadata(FileStatus status, Long length, List<RowGroupMetadata> rowGroups,
        Map<ColumnTypeMetadata.Key, ColumnTypeMetadata> columnTypeInfo) {
      this.status = status;
      this.path = status.getPath().toString();
      this.length = length;
      this.rowGroups = rowGroups;
      this.columnTypeInfo = columnTypeInfo;
    }

    @Override
    public String toString() {
      return String.format("path: %s rowGroups: %s", status.getPath(), rowGroups);
    }

    public FileStatus getStatus() {
      return status;
    }

    public String getPathString() {
      return path;
    }

    public Long getLength() {
      return length;
    }

    public List<RowGroupMetadata> getRowGroups() {
      return rowGroups;
    }

    private ColumnTypeMetadata getColumnTypeInfo(String[] name) {
      ColumnTypeMetadata columnMetadata = columnTypeInfo.get(new ColumnTypeMetadata.Key(name));
      if (columnMetadata == null) {
        throw new IllegalArgumentException("no column for " + Arrays.toString(name) + " in " + columnTypeInfo);
      }
      return columnMetadata;
    }

    public PrimitiveTypeName getPrimitiveType(String[] columnName) {
      return getColumnTypeInfo(columnName).primitiveType;
    }

    public OriginalType getOriginalType(String[] columnName) {
      return getColumnTypeInfo(columnName).originalType;
    }
  }


  /**
   * A struct that contains the metadata for a parquet row group
   */
  public static class RowGroupMetadata {
    private final Long start;
    private final Long length;
    private final Long rowCount;
    private final Map<String, Float> hostAffinity;
    private final List<ColumnMetadata> columns;

    public RowGroupMetadata(Long start, Long length, Long rowCount, Map<String, Float> hostAffinity,
        List<ColumnMetadata> columns) {
      this.start = start;
      this.length = length;
      this.rowCount = rowCount;
      this.hostAffinity = hostAffinity;
      this.columns = columns;
    }

    public Long getStart() {
      return start;
    }

    public Long getLength() {
      return length;
    }

    public Long getRowCount() {
      return rowCount;
    }

    public Map<String, Float> getHostAffinity() {
      return hostAffinity;
    }

    public List<ColumnMetadata> getColumns() {
      return columns;
    }
  }


  public static class ColumnTypeMetadata {
    private final String[] name;
    private final PrimitiveTypeName primitiveType;
    private final OriginalType originalType;

    public ColumnTypeMetadata(String[] name, PrimitiveTypeName primitiveType, OriginalType originalType) {
      this.name = name;
      this.primitiveType = primitiveType;
      this.originalType = originalType;
    }

    @Override
    public String toString() {
      return "ColumnTypeMetadata [name=" + Arrays.toString(name) + ", primitiveType=" + primitiveType
          + ", originalType=" + originalType + "]";
    }

    private static class Key {
      private String[] name;
      private int hashCode = 0;

      public Key(String[] name) {
        this.name = name;
      }

      @Override
      public int hashCode() {
        if (hashCode == 0) {
          hashCode = Arrays.hashCode(name);
        }
        return hashCode;
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final Key other = (Key) obj;
        return Arrays.equals(this.name, other.name);
      }

      @Override
      public String toString() {
        String s = null;
        for (String namePart : name) {
          if (s != null) {
            s += ".";
            s += namePart;
          } else {
            s = namePart;
          }
        }
        return s;
      }
    }
  }


  /**
   * A struct that contains the metadata for a column in a parquet file.
   */
  public static class ColumnMetadata {
    // Use a string array for name instead of Schema Path to make serialization easier
    private final String[] name;
    private final Long nulls;
    private Object mxValue;

    public ColumnMetadata(String[] name, Object mxValue, Long nulls) {
      this.name = name;
      this.mxValue = mxValue;
      this.nulls = nulls;
    }

    public void setMax(Object mxValue) {
      this.mxValue = mxValue;
    }

    public String[] getName() {
      return name;
    }

    public Long getNulls() {
      return nulls;
    }

    public boolean hasSingleValue() {
      return (mxValue != null);
    }

    public Object getMaxValue() {
      return mxValue;
    }

    public void setMin(Object newMin) {
      // noop - min value not stored in this version of the metadata
    }
  }
}


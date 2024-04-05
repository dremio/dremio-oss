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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.parquet2.LogicalListL1Converter;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.exec.work.ExecErrorConstants;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.joda.time.Chronology;
import org.joda.time.DateTimeConstants;

/** Utility class where we can capture common logic between the two parquet readers */
public final class ParquetReaderUtility {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ParquetReaderUtility.class);

  /**
   * Number of days between Julian day epoch (January 1, 4713 BC) and Unix day epoch (January 1,
   * 1970). The value of this constant is {@value}.
   */
  public static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;

  /**
   * All old parquet files (which haven't "is.date.correct=true" property in metadata) have a
   * corrupt date shift: {@value} days or 2 * {@value #JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH}
   */
  public static final long CORRECT_CORRUPT_DATE_SHIFT = 2 * JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH;

  // The year 5000 (or 1106685 day from Unix epoch) is chosen as the threshold for auto-detecting
  // date corruption.
  // This balances two possible cases of bad auto-correction. External tools writing dates in the
  // future will not
  // be shifted unless they are past this threshold (and we cannot identify them as external files
  // based on the metadata).
  // On the other hand, historical dates written with Drill wouldn't risk being incorrectly shifted
  // unless they were
  // something like 10,000 years in the past.
  private static final Chronology UTC = org.joda.time.chrono.ISOChronology.getInstanceUTC();
  public static final int DATE_CORRUPTION_THRESHOLD =
      (int) (UTC.getDateTimeMillis(5000, 1, 1, 0) / DateTimeConstants.MILLIS_PER_DAY);

  /**
   * For most recently created parquet files, we can determine if we have corrupted dates (see
   * DRILL-4203) based on the file metadata. For older files that lack statistics we must actually
   * test the values in the data pages themselves to see if they are likely corrupt.
   */
  public enum DateCorruptionStatus {
    META_SHOWS_CORRUPTION(
        DateBehavior.FIX,
        "It is determined from metadata that the date values are definitely CORRUPT"),
    META_SHOWS_NO_CORRUPTION(
        DateBehavior.NORMAL,
        "It is determined from metadata that the date values are definitely CORRECT"),
    META_UNCLEAR_TEST_VALUES(
        DateBehavior.DETECT,
        "Not enough info in metadata, parquet reader will test individual date values");

    @SuppressWarnings("checkstyle:VisibilityModifier")
    public final DateBehavior behavior;

    public final String info;

    DateCorruptionStatus(DateBehavior behavior, String info) {
      this.info = info;
      this.behavior = behavior;
    }

    @Override
    public String toString() {
      return info;
    }
  }

  public static void checkDecimalTypeEnabled(OptionManager options) {
    if (!options.getOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY).getBoolVal()) {
      throw UserException.unsupportedError()
          .message(ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG)
          .build(logger);
    }
  }

  public static Map<String, SchemaElement> getColNameToSchemaElementMapping(
      org.apache.parquet.hadoop.metadata.FileMetaData metaData, BlockMetaData blockMetaData) {
    ParquetMetadata footer = new ParquetMetadata(metaData, Lists.newArrayList(blockMetaData));
    Map<String, SchemaElement> schemaElements = new HashMap<>();
    FileMetaData fileMetaData =
        new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    for (SchemaElement se : fileMetaData.getSchema()) {
      schemaElements.put(se.getName(), se);
    }
    return schemaElements;
  }

  public static int autoCorrectCorruptedDate(int corruptedDate) {
    return (int) (corruptedDate - CORRECT_CORRUPT_DATE_SHIFT);
  }

  /** Check for corrupted dates in a parquet file. See DRILL-4203 */
  public static DateCorruptionStatus detectCorruptDates(
      MutableParquetMetadata footer, List<SchemaPath> columns, boolean autoCorrectCorruptDates) {
    // old drill files have "parquet-mr" as created by string, and no drill version, need to check
    // min/max values to see
    // if they look corrupt
    //  - option to disable this auto-correction based on the date values, in case users are storing
    // these
    //    dates intentionally

    // migrated parquet files have 1.8.1 parquet-mr version with drill-r0 in the part of the name
    // usually containing "SNAPSHOT"

    // new parquet files are generated with "is.date.correct" property have no corruption dates

    String createdBy = footer.getFileMetaData().getCreatedBy();
    String dremioVersion =
        footer
            .getFileMetaData()
            .getKeyValueMetaData()
            .get(ParquetRecordWriter.DREMIO_VERSION_PROPERTY);
    String drillVersion =
        footer
            .getFileMetaData()
            .getKeyValueMetaData()
            .get(ParquetRecordWriter.DRILL_VERSION_PROPERTY);
    String isDateCorrect =
        footer
            .getFileMetaData()
            .getKeyValueMetaData()
            .get(ParquetRecordWriter.IS_DATE_CORRECT_PROPERTY);
    String writerVersionValue =
        footer
            .getFileMetaData()
            .getKeyValueMetaData()
            .get(ParquetRecordWriter.WRITER_VERSION_PROPERTY);
    logger.debug(
        "Detecting corrupt dates for file created by {}, dremio version {}, writer version value {}, auto correct dates {}",
        createdBy,
        dremioVersion,
        writerVersionValue,
        autoCorrectCorruptDates);
    if (dremioVersion != null || drillVersion != null) {
      // File is generated by either Drill >= 1.3.0 or Dremio (all versions)

      if (writerVersionValue != null && Integer.parseInt(writerVersionValue) >= 2) {
        // If Drill parquet writer version is >=2 -> No date corruption.
        //   1. All parquet files written by Drill version >= 1.10.0 (DRILL-4980)
        return DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
      }

      if (Boolean.valueOf(isDateCorrect)) {
        // If the footer contains "is.date.correct" -> No date corruption.
        //   1. File generated by Drill 1.9.0 (DRILL-4203) - This property got removed in 1.10.0
        // (DRILL-4980)
        //   2. All parquet files generated by Dremio
        return DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
      }

      // File is generated using Drill >= 1.3.0 and Drill <= 1.9.0
      return DateCorruptionStatus.META_SHOWS_CORRUPTION;
    } else {
      // Possibly an old, un-migrated Drill file, check the column statistics to see if min/max
      // values look corrupt
      // only applies if there is a date column selected
      if (createdBy == null || "parquet-mr".equals(createdBy)) {
        // loop through parquet column metadata to find date columns, check for corrupt values
        return checkForCorruptDateValuesInStatistics(footer, columns, autoCorrectCorruptDates);
      } else {
        // check the created by to see if it is a migrated Drill file
        try {
          VersionParser.ParsedVersion parsedCreatedByVersion = VersionParser.parse(createdBy);
          // check if this is a migrated Drill file, lacking a Drill version number, but with
          // "drill" in the parquet created-by string
          if (parsedCreatedByVersion.hasSemanticVersion()) {
            SemanticVersion semVer = parsedCreatedByVersion.getSemanticVersion();
            String pre = semVer.pre + "";
            if (semVer.major == 1
                && semVer.minor == 8
                && semVer.patch == 1
                && pre.contains("drill")) {
              return DateCorruptionStatus.META_SHOWS_CORRUPTION;
            }
          }
          // written by a tool that wasn't Drill, the dates are not corrupted
          return DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
        } catch (VersionParser.VersionParseException e) {
          // If we couldn't parse "created by" field, check column metadata of date columns
          return checkForCorruptDateValuesInStatistics(footer, columns, autoCorrectCorruptDates);
        }
      }
    }
  }

  /**
   * Detect corrupt date values by looking at the min/max values in the metadata.
   *
   * <p>This should only be used when a file does not have enough metadata to determine if the data
   * was written with an older version of Drill, or an external tool. Drill versions 1.3 and beyond
   * should have enough metadata to confirm that the data was written by Drill.
   *
   * <p>This method only checks the first Row Group, because Drill has only ever written a single
   * Row Group per file.
   *
   * @param footer
   * @param columns
   * @param autoCorrectCorruptDates user setting to allow enabling/disabling of auto-correction of
   *     corrupt dates. There are some rare cases (storing dates thousands of years into the future,
   *     with tools other than Drill writing files) that would result in the date values being
   *     "corrected" into bad values.
   */
  public static DateCorruptionStatus checkForCorruptDateValuesInStatistics(
      MutableParquetMetadata footer, List<SchemaPath> columns, boolean autoCorrectCorruptDates) {
    // Users can turn-off date correction in cases where we are detecting corruption based on the
    // date values
    // that are unlikely to appear in common datasets. In this case report that no correction needs
    // to happen
    // during the file read
    if (!autoCorrectCorruptDates) {
      return DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
    }
    // Drill produced files have only ever have a single row group, if this changes in the future it
    // won't matter
    // as we will know from the Drill version written in the files that the dates are correct

    BlockMetaData blockMetaData =
        (footer.getBlocks().size() > 0) ? footer.getBlocks().get(0) : null;
    if (blockMetaData == null) {
      return DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
    }
    Map<String, SchemaElement> schemaElements =
        ParquetReaderUtility.getColNameToSchemaElementMapping(
            footer.getFileMetaData(), blockMetaData);
    findDateColWithStatsLoop:
    for (SchemaPath schemaPath : columns) {
      List<ColumnDescriptor> parquetColumns = footer.getFileMetaData().getSchema().getColumns();
      for (int i = 0; i < parquetColumns.size(); ++i) {
        ColumnDescriptor column = parquetColumns.get(i);
        // this reader only supports flat data, this is restricted in the ParquetScanBatchCreator
        // creating a NameSegment makes sure we are using the standard code for comparing names,
        // currently it is all case-insensitive
        if (ColumnUtils.isStarQuery(columns)
            || new PathSegment.NameSegment(column.getPath()[0])
                .equals(schemaPath.getRootSegment())) {
          ColumnChunkMetaData columnChunkMetaData = null;
          ConvertedType convertedType = schemaElements.get(column.getPath()[0]).getConverted_type();
          if (convertedType != null && convertedType.equals(ConvertedType.DATE)) {
            List<ColumnChunkMetaData> colChunkList = blockMetaData.getColumns();
            for (int j = 0; j < colChunkList.size(); j++) {
              if (colChunkList.get(j).getPath().equals(ColumnPath.get(column.getPath()))) {
                columnChunkMetaData = colChunkList.get(j);
                break;
              }
            }
          }
          if (columnChunkMetaData == null) {
            // column does not appear in this file, skip it
            continue;
          }
          Statistics statistics = columnChunkMetaData.getStatistics();
          Integer max = (Integer) statistics.genericGetMax();
          if (statistics.hasNonNullValue()) {
            if (max > ParquetReaderUtility.DATE_CORRUPTION_THRESHOLD) {
              return DateCorruptionStatus.META_SHOWS_CORRUPTION;
            }
          } else {
            // no statistics, go check the first page
            return DateCorruptionStatus.META_UNCLEAR_TEST_VALUES;
          }
        }
      }
    }
    return DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
  }

  /**
   * Converts {@link ColumnDescriptor} to {@link SchemaPath} and converts any parquet LOGICAL LIST
   * to something the execution engine can understand (removes the extra 'list' and 'element' fields
   * from the name)
   */
  public static List<String> convertColumnDescriptor(
      final MessageType schema, final ColumnDescriptor columnDescriptor) {
    List<String> path = Lists.newArrayList(columnDescriptor.getPath());

    // go through the path and find all logical lists
    int index = 0;
    Type type = schema;
    while (!type
        .isPrimitive()) { // don't bother checking the last element in the path as it is a primitive
      // type
      type = type.asGroupType().getType(path.get(index));
      if (type.getOriginalType() == OriginalType.LIST
          && LogicalListL1Converter.isSupportedSchema(type.asGroupType())) {
        // remove 'list'
        type = type.asGroupType().getType(path.get(index + 1));
        path.remove(index + 1);

        // remove 'element'
        type = type.asGroupType().getType(path.get(index + 1));

        // handle nested list case
        while (type.getOriginalType() == OriginalType.LIST
            && LogicalListL1Converter.isSupportedSchema(type.asGroupType())) {
          // current 'list'.'element' entry
          path.remove(index + 1);

          // nested 'list' entry
          type = type.asGroupType().getType(path.get(index + 1));
          path.remove(index + 1);

          type = type.asGroupType().getType(path.get(index + 1));
        }

        // final 'list'.'element' entry
        path.remove(index + 1);
      }
      index++;
    }
    return path;
  }

  /**
   * Utilities for converting from parquet INT96 binary (impala, hive timestamp) to date time value.
   * This utilizes the Joda library.
   */
  public static class NanoTimeUtils {

    public static final long NANOS_PER_MILLISECOND = 1000000;

    /**
     * @param binaryTimeStampValue hive, impala timestamp values with nanoseconds precision are
     *     stored in parquet Binary as INT96 (12 constant bytes)
     * @return Unix Timestamp - the number of milliseconds since January 1, 1970, 00:00:00 GMT
     *     represented by @param binaryTimeStampValue .
     */
    public static long getDateTimeValueFromBinary(Binary binaryTimeStampValue) {
      // This method represents binaryTimeStampValue as ByteBuffer, where timestamp is stored as sum
      // of
      // julian day number (32-bit) and nanos of day (64-bit)
      NanoTime nt = NanoTime.fromBinary(binaryTimeStampValue);
      int julianDay = nt.getJulianDay();
      long nanosOfDay = nt.getTimeOfDayNanos();
      return (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * DateTimeConstants.MILLIS_PER_DAY
          + nanosOfDay / NANOS_PER_MILLISECOND;
    }
  }

  /**
   * Get the list of row group numbers for given file input split. Logic used here is same as how
   * Hive's parquet input format finds the row group numbers for input split.
   */
  public static List<Integer> getRowGroupNumbersFromFileSplit(
      final long splitStart, final long splitLength, final MutableParquetMetadata footer)
      throws IOException {
    final List<BlockMetaData> blocks = footer.getBlocks();
    final List<Integer> rowGroupNums = Lists.newArrayList();

    int i = 0;
    for (final BlockMetaData block : blocks) {
      if (block != null) {
        final long firstDataPage = getFirstPageOffset(block.getColumns().get(0));
        if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
          rowGroupNums.add(i);
        }
      }
      i++;
    }

    return rowGroupNums;
  }

  /**
   * Returns the offset of the first page in the column chunk disregarding the page type. (In case
   * of dictionary encoding it will be the offset of the dictionary page otherwise the offset of the
   * first data page.)
   */
  public static long getFirstPageOffset(ColumnChunkMetaData columnChunk) {
    // It seems ColumnChunkMetaData.getStartingPos() in parquet-mr does not handle all the special
    // cases
    long dictPageOffset = columnChunk.getDictionaryPageOffset();
    long firstDataPageOffset = columnChunk.getFirstDataPageOffset();

    // 0 offset is invalid because of the first MAGIC bytes at the beginning of a Parquet file. 0
    // usually means unset.
    if (dictPageOffset > 0) {
      // Normal case: dictionary page is before the first data page
      // Special case: only dictionary page exist; no values are encoded but dictionary page got
      // written somehow
      if (dictPageOffset < firstDataPageOffset || firstDataPageOffset <= 0) {
        return dictPageOffset;
      }
    }
    // In any other case we return the first data page.
    // (Since dictPageOffset is either unset or invalid we cannot do anything smarter anyway.)
    return firstDataPageOffset;
  }

  public static List<SchemaPath> getColumnsFromEqualityIds(
      List<Integer> equalityIds, List<IcebergProtobuf.IcebergSchemaField> icebergColumnIds) {
    return equalityIds.stream()
        .map(id -> icebergColumnIds.stream().filter(col -> col.getId() == id).findFirst())
        .filter(Optional::isPresent)
        .map(col -> SchemaPath.getSimplePath(col.get().getSchemaPath()))
        .collect(Collectors.toList());
  }

  private ParquetReaderUtility() {
    // Utility class
  }
}

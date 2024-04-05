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
package com.dremio.dac.explore;

import static com.dremio.common.util.DateTimes.toMillis;
import static com.dremio.common.utils.SqlUtils.quoteIdentifier;
import static com.dremio.common.utils.SqlUtils.stringLiteral;
import static com.dremio.dac.proto.model.dataset.DataType.FLOAT;
import static com.dremio.dac.proto.model.dataset.DataType.INTEGER;
import static com.dremio.dac.proto.model.dataset.DataType.TEXT;
import static java.lang.String.format;

import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.dac.explore.model.Column;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.HistogramValue;
import com.dremio.dac.explore.model.extract.Selection;
import com.dremio.dac.model.job.JobData;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class used to generate historgrams. */
class HistogramGenerator {

  private static final Logger logger = LoggerFactory.getLogger(HistogramGenerator.class);

  private static final int BATCH_SIZE = 500; // size of batch examined in each iteration
  static final int BUCKETS = 200;

  private final QueryExecutor executor;

  public HistogramGenerator(QueryExecutor executor) {
    this.executor = executor;
  }

  /** Histogram data */
  public static class Histogram<V> {

    private List<V> values;
    private long availableValues;

    public Histogram(List<V> values, long availableValues) {
      super();
      this.values = values;
      this.availableValues = availableValues;
    }

    public List<V> getValues() {
      return values;
    }

    public long getAvailableValues() {
      return availableValues;
    }
  }

  public Histogram<HistogramValue> getHistogram(
      final DatasetPath datasetPath,
      DatasetVersion version,
      Selection selection,
      DataType colType,
      SqlQuery datasetQuery,
      BufferAllocator allocator) {
    final String datasetPreviewJobResultsTable =
        DatasetsUtil.getDatasetPreviewJob(executor, datasetQuery, datasetPath, version)
            .getJobResultsTable();

    final String colName = selection.getColName();
    final int myBuckets = BUCKETS;

    // run preliminary job to get MIN, MAX, AVG, STDEV for colName to be able to do
    // correct rounding and calculate ranges
    StringBuilder prelimValuesQueryBuilder = new StringBuilder();
    final String colNameClean = quoteIdentifier(colName);
    int offset = 0;
    final StringBuilder hgQueryBuilder = new StringBuilder();

    double lowerBoundary = Double.MIN_VALUE;
    double upperBoundary = Double.MAX_VALUE;
    double range = Double.MIN_VALUE;
    List<Number> ranges = new ArrayList<>();

    JobDataFragment prelimData = null;
    boolean isBinned = false;

    final String projectedColName = "dremio_value";

    hgQueryBuilder.append("SELECT\n");
    hgQueryBuilder.append(
        format(
            "  dremio_values_table.%s as %s,\n  COUNT(*) as dremio_value_count\n",
            colNameClean, projectedColName));
    hgQueryBuilder.append(
        format("FROM %s AS dremio_values_table \n", datasetPreviewJobResultsTable));
    hgQueryBuilder.append(format("GROUP BY dremio_values_table.%s\n", colNameClean));
    hgQueryBuilder.append("ORDER BY dremio_value_count DESC");

    switch (colType) {
      case TIME:
      case DATE:
      case DATETIME:
      case INTEGER:
      case FLOAT:
        try {
          prelimValuesQueryBuilder.append("SELECT\n");
          prelimValuesQueryBuilder.append(
              format(
                  " MIN(dremio_values_table.%s) as colMin, MAX(dremio_values_table.%s) as colMax\n",
                  colNameClean, colNameClean));
          prelimValuesQueryBuilder.append(
              format(" FROM %s AS dremio_values_table\n", datasetPreviewJobResultsTable));

          final SqlQuery prelimQuery =
              datasetQuery.cloneWithNewSql(prelimValuesQueryBuilder.toString());
          // need to get values and calculate everything
          prelimData =
              executor
                  .runQueryAndWaitForCompletion(
                      prelimQuery, QueryType.UI_INTERNAL_RUN, datasetPath, version)
                  .range(allocator, offset, BATCH_SIZE);
        } catch (Throwable t) {
          logger.error(
              format(
                  "Exception while trying to get histogram. Reverting to simple group %s by value",
                  colNameClean),
              t);
        }
        break;
      default:
        break;
    }
    try {
      if (prelimData != null
          && (colType == DataType.TIME
              || colType == DataType.DATE
              || colType == DataType.DATETIME)) {
        try {
          // Read the values in batches - should be a single row
          if (prelimData.getReturnedRowCount() > 0) {
            // need to map type to SqlTypeName to get only Numeric family
            // deal separately with dates (can use UNIX_TIMESTAMP)
            LocalDateTime min = (LocalDateTime) prelimData.extractValue("colMin", 0);

            LocalDateTime max = (LocalDateTime) prelimData.extractValue("colMax", 0);

            long duration = toMillis(max) - toMillis(min);
            range = duration / 1000L; // get seconds
            range /= myBuckets;
            range = Math.round(range);

            TruncEvalEnum trucateTo = TruncEvalEnum.SECOND;

            if (range > 1) {
              for (TruncEvalEnum value : TruncEvalEnum.getSortedAscValues()) {
                double tmpRange = range / value.getDivisor();
                if (tmpRange <= 1) {
                  trucateTo = value;
                  break;
                }
              }
            }

            produceRanges(ranges, min, max, trucateTo);

            /* Example of the query
            select count(*), date_trunc('DAY', B)
            from yellow_tripdata_2009_01_100000_conv_full group by date_trunc('DAY', B)
            */

            hgQueryBuilder.setLength(0);
            hgQueryBuilder.append("SELECT\n");
            hgQueryBuilder.append(
                format(
                    "date_trunc('%s', dremio_values_table.%s) as %s,\n",
                    trucateTo.name, colNameClean, projectedColName));
            hgQueryBuilder.append("COUNT(*) as dremio_value_count\n");
            hgQueryBuilder.append(
                format(" FROM %s AS dremio_values_table\n", datasetPreviewJobResultsTable));
            hgQueryBuilder.append("GROUP BY\n");
            hgQueryBuilder.append(
                format("date_trunc('%s', dremio_values_table.%s)\n", trucateTo.name, colNameClean));
            hgQueryBuilder.append(format("ORDER BY %s ASC", projectedColName));
            isBinned = true;
          } else {
            throw new ClientErrorException(
                "no data returned for query: " + prelimValuesQueryBuilder.toString());
          }
        } catch (Throwable t) {
          logger.error(
              format(
                  "Exception while trying to get histogram. Reverting to simple group %s by value",
                  colNameClean),
              t);
        }
      }

      if (prelimData != null && (colType == DataType.FLOAT || colType == DataType.INTEGER)) {
        try {
          // Read the values in batches - should be a single row
          Double min;
          Double max;
          if (prelimData.getReturnedRowCount() > 0) {
            min = Double.parseDouble(prelimData.extractString("colMin", 0));
            max = Double.parseDouble(prelimData.extractString("colMax", 0));
          } else {
            throw new ClientErrorException(
                "no data returned for query: " + prelimValuesQueryBuilder.toString());
          }

          upperBoundary = max;
          lowerBoundary = min;
          double value = lowerBoundary;

          double boundaries =
              Math.abs(
                  upperBoundary
                      - lowerBoundary); // make sure it is > 0, though it should be in any case
          if (boundaries == 0.0d) {
            ranges.add(value);
          }
          range = boundaries / myBuckets;
          long roundedRange = Math.round(range);
          if (colType == DataType.INTEGER) {
            if (roundedRange == 0L) {
              if (boundaries > 1d) {
                roundedRange = 1L;
              } else {
                ranges.add(value);
              }
            }
            range = roundedRange;
          }
          if (ranges.isEmpty()) {
            while (value < upperBoundary) {
              ranges.add(value);
              value += range;
            }
            ranges.add(upperBoundary);
            // adding one more range to avoid boundary conditions for exact numbers
            ranges.add(upperBoundary + range);
          }

          if (range > 0.0d) {
            hgQueryBuilder.setLength(0);
            hgQueryBuilder.append("SELECT\n");
            hgQueryBuilder.append(
                format(
                    "  ROUND(CAST(dremio_values_table.%s AS DOUBLE)/%f)*%f as %s,\n  COUNT(*) as dremio_value_count\n",
                    colNameClean, range, range, projectedColName));
            hgQueryBuilder.append(
                format("FROM %s AS dremio_values_table\n", datasetPreviewJobResultsTable));
            // hgQueryBuilder.append(format(" WHERE %s < %s AND %s > %s\n", colNameClean,
            // upperBoundary, colNameClean, lowerBoundary));
            hgQueryBuilder.append(
                format(
                    " GROUP BY ROUND(CAST(dremio_values_table.%s AS DOUBLE)/%f)*%f\n",
                    colNameClean, range, range));
            hgQueryBuilder.append(format("ORDER BY %s ASC", projectedColName));
            isBinned = true;
          }
        } catch (Throwable t) {
          logger.error(
              format(
                  "Exception while trying to get histogram. Reverting to simple group %s by value",
                  colNameClean),
              t);
        }
      }
    } finally {
      if (prelimData != null) {
        prelimData.close();
      }
    }
    // we got rounding number
    final SqlQuery hgQuery = datasetQuery.cloneWithNewSql(hgQueryBuilder.toString());

    final JobData completeJobData =
        executor.runQueryAndWaitForCompletion(
            hgQuery, QueryType.UI_INTERNAL_RUN, datasetPath, version);

    final List<HistogramValue> values = new ArrayList<>();
    long total = 0;
    offset = 0;

    while (true) {
      try (JobDataFragment data = completeJobData.range(allocator, offset, BATCH_SIZE)) {
        if (data.getReturnedRowCount() <= 0) {
          break;
        }
        final String countCol = "dremio_value_count";
        int rangeCount = 1;
        for (int i = 0; i < data.getReturnedRowCount(); i++) {
          total += (Long) data.extractValue(countCol, i);

          DataType type = data.getColumn(projectedColName).getType();
          if (type == null) {
            type = data.extractType(projectedColName, i);
          }
          String dataValueStr = data.extractString(projectedColName, i);

          HistogramValue.ValueRange valueRange = null;
          if (isBinned) {
            if (dataValueStr == null) {
              continue;
            }
            if (rangeCount >= ranges.size()) {
              logger.error(
                  "Number of ranges {} is not enough to cover full spectrum. Could be because of boundary conditions",
                  rangeCount);
              throw new ClientErrorException(
                  "Number of ranges "
                      + rangeCount
                      + " is not enough to cover full spectrum. Could be because of boundary conditions");
            }

            // fill out empty bins as well
            if (type == DataType.FLOAT) {
              double dataValue = Double.parseDouble(dataValueStr);
              while (dataValue >= (Double) ranges.get(rangeCount)) {
                valueRange =
                    new HistogramValue.ValueRange(
                        ranges.get(rangeCount - 1), ranges.get(rangeCount));
                values.add(
                    new HistogramValue(
                        colType,
                        ranges.get(rangeCount - 1).toString(),
                        /* percent= */ 0.0,
                        0,
                        valueRange));
                rangeCount++;
              }
            } else if (colType == DataType.TIME
                || colType == DataType.DATE
                || colType == DataType.DATETIME) {
              LocalDateTime dataValue = (LocalDateTime) data.extractValue(projectedColName, i);
              while (toMillis(dataValue) >= (Long) ranges.get(rangeCount)) {
                valueRange =
                    new HistogramValue.ValueRange(
                        ranges.get(rangeCount - 1), ranges.get(rangeCount));
                values.add(
                    new HistogramValue(
                        colType,
                        ranges.get(rangeCount - 1).toString(),
                        /* percent= */ 0.0,
                        0,
                        valueRange));
                rangeCount++;
              }
              dataValueStr = Long.toString(toMillis(dataValue));
            }
            valueRange =
                new HistogramValue.ValueRange(ranges.get(rangeCount - 1), ranges.get(rangeCount));
            rangeCount++;
          }
          long countValue = (long) data.extractValue(countCol, i);

          // Set the percent later once all the rows are examined.
          values.add(
              new HistogramValue(
                  colType,
                  dataValueStr,
                  /* percent= */ 0.0,
                  countValue,
                  (valueRange == null)
                      ? new HistogramValue.ValueRange(dataValueStr, dataValueStr)
                      : valueRange));
        }

        // Move onto next set of records
        offset += data.getReturnedRowCount();
      }
    }

    // Set the percent values once all rows in dataset preview output are examined.
    for (HistogramValue hgValue : values) {
      hgValue.setPercent((hgValue.getCount() * 100d) / total);
    }

    return new Histogram<>(values, total);
  }

  @VisibleForTesting
  static void produceRanges(
      List<Number> ranges, LocalDateTime min, LocalDateTime max, TruncEvalEnum truncateTo) {
    long timeValue = toMillis(roundTime(min, truncateTo, true));
    long maxTimeValue = toMillis(roundTime(max, truncateTo, false));
    ranges.add(timeValue);
    // adding one more range to alleviate boundary conditions
    while (timeValue <= maxTimeValue) {
      LocalDateTime tmpValue = new LocalDateTime(timeValue, DateTimeZone.UTC);
      switch (truncateTo) {
        case SECOND:
          timeValue = toMillis(tmpValue.plusSeconds(1));
          break;
        case MINUTE:
          timeValue = toMillis(tmpValue.plusMinutes(1));
          break;
        case HOUR:
          timeValue = toMillis(tmpValue.plusHours(1));
          break;
        case DAY:
          timeValue = toMillis(tmpValue.plusDays(1));
          break;
        case WEEK:
          timeValue = toMillis(tmpValue.plusWeeks(1));
          break;
        case MONTH:
          timeValue = toMillis(tmpValue.plusMonths(1));
          break;
        case QUARTER:
          timeValue = toMillis(tmpValue.plusMonths(3));
          break;
        case YEAR:
          timeValue = toMillis(tmpValue.plusYears(1));
          break;
        case DECADE:
          timeValue = toMillis(tmpValue.plusYears(10));
          break;
        case CENTURY:
          timeValue = toMillis(tmpValue.plusYears(100));
          break;
        case MILLENNIUM:
          timeValue = toMillis(tmpValue.plusYears(1000));
          break;
        default:
          break;
      }
      ranges.add(timeValue);
      if (ranges.size() > 100_000) {
        throw new AssertionError(
            String.format(
                "the ranges size should not grow that big: min: %s, max: %s, t: %s",
                min, max, truncateTo));
      }
    }
  }

  @VisibleForTesting
  protected static LocalDateTime roundTime(
      LocalDateTime tmpValue, TruncEvalEnum trucateTo, boolean isRoundDown) {
    switch (trucateTo) {
      case SECOND:
        if (isRoundDown) {
          return tmpValue.secondOfMinute().roundFloorCopy();
        } else {
          return tmpValue.secondOfMinute().roundCeilingCopy();
        }
      case MINUTE:
        if (isRoundDown) {
          return tmpValue.minuteOfHour().roundFloorCopy();
        } else {
          return tmpValue.minuteOfHour().roundCeilingCopy();
        }
      case HOUR:
        if (isRoundDown) {
          return tmpValue.hourOfDay().roundFloorCopy();
        } else {
          return tmpValue.hourOfDay().roundCeilingCopy();
        }
      case DAY:
        if (isRoundDown) {
          return tmpValue.dayOfMonth().roundFloorCopy();
        } else {
          return tmpValue.dayOfMonth().roundCeilingCopy();
        }
      case WEEK:
        if (isRoundDown) {
          return tmpValue.weekOfWeekyear().roundFloorCopy();
        } else {
          return tmpValue.weekOfWeekyear().roundCeilingCopy();
        }
      case MONTH:
        if (isRoundDown) {
          return tmpValue.monthOfYear().roundFloorCopy();
        } else {
          return tmpValue.monthOfYear().roundCeilingCopy();
        }
      case QUARTER:
        if (isRoundDown) {
          LocalDateTime roundeddown = tmpValue.monthOfYear().roundFloorCopy();
          int month = roundeddown.getMonthOfYear();
          int quarter = (month - 1) % 3;
          return roundeddown.minusMonths(quarter);
        } else {
          LocalDateTime roundedUp = tmpValue.monthOfYear().roundCeilingCopy();
          int month = roundedUp.getMonthOfYear();
          int quarter = 3 - (month - 1) % 3;
          return roundedUp.plusMonths(quarter);
        }
      case YEAR:
        if (isRoundDown) {
          return tmpValue.year().roundFloorCopy();
        } else {
          return tmpValue.year().roundCeilingCopy();
        }
      case DECADE:
        if (isRoundDown) {
          LocalDateTime roundeddown = tmpValue.year().roundFloorCopy();
          int year = roundeddown.getYear();
          int roundedDownYear = year % 10;
          return roundeddown.minusYears(roundedDownYear);
        } else {
          LocalDateTime roundedUp = tmpValue.year().roundCeilingCopy();
          int year = roundedUp.getYear();
          int roundedUpYear = 10 - year % 10;
          return roundedUp.plusYears(roundedUpYear);
        }
      case CENTURY:
        if (isRoundDown) {
          LocalDateTime roundeddown = tmpValue.year().roundFloorCopy();
          int year = roundeddown.getYear();
          int roundedDownYear = year % 100;
          return roundeddown.minusYears(roundedDownYear);
        } else {
          LocalDateTime roundedUp = tmpValue.year().roundCeilingCopy();
          int year = roundedUp.getYear();
          int roundedUpYear = 100 - year % 100;
          return roundedUp.plusYears(roundedUpYear);
        }
      case MILLENNIUM:
        if (isRoundDown) {
          LocalDateTime roundeddown = tmpValue.year().roundFloorCopy();
          int year = roundeddown.getYear();
          int roundedDownYear = year % 1000;
          return roundeddown.minusYears(roundedDownYear);
        } else {
          LocalDateTime roundedUp = tmpValue.year().roundCeilingCopy();
          int year = roundedUp.getYear();
          int roundedUpYear = 1000 - year % 1000;
          return roundedUp.plusYears(roundedUpYear);
        }
      default:
        return tmpValue;
    }
  }

  /** for type info */
  public class CleanDataHistogramValue {
    private final DataType type;
    private final String value;
    private final long count;
    private final Map<String, Boolean> isCleanMap;

    private double percent;

    public CleanDataHistogramValue(
        DataType type, String value, double percent, long count, Map<String, Boolean> isCleanMap) {
      super();
      this.type = type;
      this.value = value;
      this.percent = percent;
      this.count = count;
      this.isCleanMap = isCleanMap;
    }

    public DataType getType() {
      return type;
    }

    public String getValue() {
      return value;
    }

    public double getPercent() {
      return percent;
    }

    public long getCount() {
      return count;
    }

    public Boolean isClean(boolean cast, DataType type) {
      return isCleanMap.get(isCleanFieldName(cast, type));
    }

    public HistogramValue toHistogramValue() {
      return new HistogramValue(type, value, percent, count, null);
    }

    public void setPercent(double percent) {
      this.percent = percent;
    }
  }

  private static String isCleanFieldName(boolean cast, DataType type) {
    return "dremio_is_clean_" + (cast ? "cast_" : "") + type.name();
  }

  public Histogram<CleanDataHistogramValue> getCleanDataHistogram(
      final DatasetPath datasetPath,
      DatasetVersion version,
      String colName,
      SqlQuery datasetQuery,
      BufferAllocator allocator) {

    final String datasetPreviewJobResultsTable =
        DatasetsUtil.getDatasetPreviewJob(executor, datasetQuery, datasetPath, version)
            .getJobResultsTable();

    boolean[] casts = {true, false};
    // Types currently supported by clean type
    DataType[] types = {TEXT, INTEGER, FLOAT};

    String quotedColName = format("%s", quoteIdentifier(colName));

    /**
     * SQL to get the histogram
     *
     * <p>Ex query: SELECT colName AS dremio_value, is_clean_data(colName, 1, 'TEXT') AS
     * dremio_is_clean_cast_TEXT, is_clean_data(colName, 0, 'TEXT') AS dremio_is_clean_TEXT,
     * is_clean_data(colName, 1, 'INTEGER') AS dremio_is_clean_cast_INTEGER, is_clean_data(colName,
     * 0, 'INTEGER') AS dremio_is_clean_INTEGER, is_clean_data(colName, 1, 'FLOAT') AS
     * dremio_is_clean_cast_FLOAT, is_clean_data(colName, 0, 'FLOAT') AS dremio_is_clean_FLOAT,
     * typeOf(colName) AS dremio_value_type, COUNT(*) as dremio_value_count FROM
     * "__jobResultsStore"."jobsId" GROUP BY colName, typeOf(colName) ORDER BY dremio_value_count
     * DESC
     */
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT\n  ")
        .append("dremio_values_table.")
        .append(quotedColName)
        .append(" AS dremio_value,\n");
    for (DataType dataType : types) {
      for (boolean c : casts) {
        sb.append("  is_clean_data(")
            .append("dremio_values_table.")
            .append(quotedColName)
            .append(", ")
            .append(c ? 1 : 0)
            .append(", '")
            .append(dataType.name())
            .append("')");
        sb.append(" as ").append(isCleanFieldName(c, dataType)).append(",\n");
      }
    }
    sb.append("  typeOf(")
        .append("dremio_values_table.")
        .append(quotedColName)
        .append(") AS dremio_value_type,\n");
    sb.append("  COUNT(*) as dremio_value_count\n");
    sb.append(" FROM ").append(datasetPreviewJobResultsTable).append(" AS dremio_values_table\n");
    sb.append(" GROUP BY ")
        .append("dremio_values_table.")
        .append(quotedColName)
        .append(", typeOf(")
        .append("dremio_values_table.")
        .append(quotedColName)
        .append(")\n");
    sb.append(" ORDER BY dremio_value_count DESC");

    // get the result
    JobData completeJobData =
        executor.runQueryAndWaitForCompletion(
            datasetQuery.cloneWithNewSql(sb.toString()),
            QueryType.UI_INTERNAL_RUN,
            datasetPath,
            version);

    final String colCount = "dremio_value_count";
    long total = 0;
    int offset = 0;
    List<CleanDataHistogramValue> values = new ArrayList<>();

    while (true) {
      try (final JobDataFragment data = completeJobData.range(allocator, offset, BATCH_SIZE)) {
        if (data.getReturnedRowCount() <= 0) {
          break;
        }
        Column selected = data.getColumn("dremio_value");
        if (selected == null) {
          throw new ClientErrorException("column not found in data: " + colName);
        }

        for (int i = 0; i < data.getReturnedRowCount(); i++) {
          Map<String, Boolean> isCleanMap = new HashMap<>();
          for (DataType dataType : types) {
            for (boolean c : casts) {
              String cleanFieldName = isCleanFieldName(c, dataType);
              isCleanMap.put(cleanFieldName, (Boolean) data.extractValue(cleanFieldName, i));
            }
          }
          DataType type = data.extractType(selected.getName(), i);
          if (type == null) {
            type = selected.getType();
          }
          long countValue = (long) data.extractValue(colCount, i);

          total += countValue;

          // Set the percent later once all the rows are examined.
          values.add(
              new CleanDataHistogramValue(
                  type,
                  data.extractString(selected.getName(), i),
                  /* percent= */ 0.0d,
                  countValue,
                  isCleanMap));
        }

        // Move onto next set of records
        offset += data.getReturnedRowCount();
      }
    }

    // Set the percent values once all rows in dataset preview output are examined.
    for (CleanDataHistogramValue hgValue : values) {
      hgValue.setPercent((hgValue.getCount() * 100d) / total);
    }

    return new Histogram<>(values, total);
  }

  public Map<DataType, Long> getTypeHistogram(
      final DatasetPath datasetPath,
      DatasetVersion version,
      String colName,
      SqlQuery datasetQuery,
      BufferAllocator allocator) {
    final String datasetPreviewJobTableResults =
        DatasetsUtil.getDatasetPreviewJob(executor, datasetQuery, datasetPath, version)
            .getJobResultsTable();

    String quotedColName = quoteIdentifier(colName);
    String newSql =
        format(
            "SELECT typeOf(dremio_values_table.%s) AS dremio_value_type, COUNT(*) as dremio_type_count FROM %s AS dremio_values_table GROUP BY typeOf(dremio_values_table.%s)",
            quotedColName, datasetPreviewJobTableResults, quotedColName);

    JobData completeJobData =
        executor.runQueryAndWaitForCompletion(
            datasetQuery.cloneWithNewSql(newSql), QueryType.UI_INTERNAL_RUN, datasetPath, version);

    final Map<DataType, Long> values = new LinkedHashMap<>();

    int offset = 0;
    while (true) {
      try (final JobDataFragment data = completeJobData.range(allocator, offset, BATCH_SIZE)) {
        if (data.getReturnedRowCount() <= 0) {
          break;
        }
        for (int i = 0; i < data.getReturnedRowCount(); i++) {
          String typeName = data.extractString("dremio_value_type", i);
          DataType dataType = DataTypeUtil.getDataType(MinorType.valueOf(typeName));
          Long existing = values.get(dataType);
          if (existing == null) {
            existing = Long.valueOf(0);
          }
          Long newValue = (Long) data.extractValue("dremio_type_count", i);
          // there are fewer DataTypes than MinorTypes
          values.put(dataType, existing + newValue);
        }

        // Move onto next set of records
        offset += data.getReturnedRowCount();
      }
    }
    return values;
  }

  @VisibleForTesting
  protected enum TruncEvalEnum {
    SECOND("SECOND", 1L),
    MINUTE("MINUTE", 60L),
    HOUR("HOUR", 3600L),
    DAY("DAY", 24L * 3600),
    WEEK("WEEK", 24L * 3600 * 7),
    MONTH("MONTH", 24L * 3600 * 30),
    QUARTER("QUARTER", 24L * 3600 * 30 * 4),
    YEAR("YEAR", 24L * 3600 * 365),
    DECADE("DECADE", 24L * 3600 * 365 * 10),
    CENTURY("CENTURY", 24L * 3600 * 365 * 10 * 10),
    MILLENNIUM("MILLENNIUM", 24L * 3600 * 365 * 10 * 10 * 10);

    private String name;
    private long divisor;

    TruncEvalEnum(String name, long divisor) {
      this.name = name;
      this.divisor = divisor;
    }

    public long getDivisor() {
      return divisor;
    }

    public static List<TruncEvalEnum> getSortedAscValues() {
      List<TruncEvalEnum> retList = new ArrayList<>();
      retList.add(TruncEvalEnum.SECOND);
      retList.add(TruncEvalEnum.MINUTE);
      retList.add(TruncEvalEnum.HOUR);
      retList.add(TruncEvalEnum.DAY);
      retList.add(TruncEvalEnum.WEEK);
      retList.add(TruncEvalEnum.MONTH);
      retList.add(TruncEvalEnum.QUARTER);
      retList.add(TruncEvalEnum.YEAR);
      retList.add(TruncEvalEnum.DECADE);
      retList.add(TruncEvalEnum.CENTURY);
      retList.add(TruncEvalEnum.MILLENNIUM);

      return retList;
    }
  }

  public long getSelectionCount(
      final DatasetPath datasetPath,
      final DatasetVersion version,
      final SqlQuery datasetQuery,
      final DataType dataType,
      final String colName,
      Set<String> selectedValues,
      BufferAllocator allocator) {

    List<String> filteredSelValue = new ArrayList<>();
    for (String selectedValue : selectedValues) {
      if (selectedValue != null && selectedValue.isEmpty()) {
        if (dataType == TEXT) {
          // filter out empty values for non-text types.
          filteredSelValue.add(selectedValue);
        }
      } else {
        filteredSelValue.add(selectedValue);
      }
    }

    if (filteredSelValue.isEmpty()) {
      return 0;
    }

    final String datasetPreviewJobResultsTable =
        DatasetsUtil.getDatasetPreviewJob(executor, datasetQuery, datasetPath, version)
            .getJobResultsTable();

    final String quotedColName = format("%s", quoteIdentifier(colName));

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT COUNT(*) as dremio_selection_count\n  FROM ");
    sb.append(datasetPreviewJobResultsTable);
    sb.append("\n  WHERE \n");
    sb.append(
        Joiner.on(" OR ")
            .join(
                FluentIterable.from(filteredSelValue)
                    .transform(
                        new Function<String, String>() {
                          @Nullable
                          @Override
                          public String apply(@Nullable String input) {
                            if (input == null) {
                              return quotedColName + " IS NULL";
                            }
                            return quotedColName + " = " + quoteLiteral(input, dataType);
                          }
                        })));

    try (final JobDataFragment dataFragment =
        executor
            .runQueryAndWaitForCompletion(
                datasetQuery.cloneWithNewSql(sb.toString()),
                QueryType.UI_INTERNAL_RUN,
                datasetPath,
                version)
            .truncate(allocator, 1)) {

      Long selectionCount = (Long) dataFragment.extractValue("dremio_selection_count", 0);
      return selectionCount;
    }
  }

  private String quoteLiteral(String value, DataType type) {
    if (type == DataType.TEXT) {
      return stringLiteral(value);
    } else if (type == DataType.DATE) {
      return "DATE " + stringLiteral(value);
    } else if (type == DataType.TIME) {
      return "TIME " + stringLiteral(value);
    } else if (type == DataType.DATETIME) {
      return "TIMESTAMP " + stringLiteral(value);
    }

    return value;
  }
}

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
package com.dremio.service.reflection.analysis;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.SystemUser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes a dataset by synchronously running a SQL statement to collect column statistics and
 * total row count. Since stats are collected as a preview job which considers only the first 10K
 * rows (leaf level limit), the stats can be grossly inaccurate.
 */
public class ReflectionAnalyzer {
  private static final Logger logger = LoggerFactory.getLogger(ReflectionAnalyzer.class);
  private static final NamespaceKey NONE_PATH = new NamespaceKey(ImmutableList.of("__none"));

  private static final Multimap<RelDataTypeFamily, StatType> DIMENSIONS = HashMultimap.create();
  private static final String COUNT_COLUMN = "__COUNT__";
  private static final String SELECT_COUNT_STAR = "COUNT(*) AS " + COUNT_COLUMN;

  static {
    DIMENSIONS.putAll(
        SqlTypeFamily.CHARACTER,
        ImmutableList.of(
            StatType.COUNT,
            StatType.COUNT_DISTINCT,
            StatType.MIN_LEN,
            StatType.MAX_LEN,
            StatType.AVG_LEN));

    DIMENSIONS.putAll(
        SqlTypeFamily.NUMERIC,
        ImmutableList.of(
            StatType.COUNT, StatType.COUNT_DISTINCT, StatType.MIN, StatType.MAX, StatType.AVG));

    DIMENSIONS.putAll(
        SqlTypeFamily.TIMESTAMP, ImmutableList.of(StatType.COUNT, StatType.COUNT_DISTINCT_DATE));

    DIMENSIONS.putAll(SqlTypeFamily.ANY, ImmutableList.of(StatType.COUNT, StatType.COUNT_DISTINCT));
  }

  private final JobsService jobsService;
  private final CatalogService catalogService;
  private final BufferAllocator bufferAllocator;

  public ReflectionAnalyzer(
      final JobsService jobsService,
      final CatalogService catalogService,
      final BufferAllocator allocator) {
    this.catalogService = Preconditions.checkNotNull(catalogService, "Catalog service is required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "Jobs service is required");
    this.bufferAllocator = Preconditions.checkNotNull(allocator, "Buffer allocator is required");
  }

  public TableStats analyze(final String datasetId) {
    final DremioTable table =
        catalogService
            .getCatalog(
                MetadataRequestOptions.of(
                    SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build()))
            .getTable(datasetId);
    Preconditions.checkNotNull(table, "Unknown datasetId %s", datasetId);
    final RelDataType rowType = table.getRowType(JavaTypeFactoryImpl.INSTANCE);

    final List<RelDataTypeField> fields =
        FluentIterable.from(rowType.getFieldList())
            .filter(
                new Predicate<RelDataTypeField>() {
                  @Override
                  public boolean apply(@Nullable final RelDataTypeField input) {
                    final RField field = TypeUtils.fromCalciteField(input);

                    return TypeUtils.isBoolean(field)
                        || TypeUtils.isTemporal(field)
                        || TypeUtils.isText(field)
                        || TypeUtils.isNumeric(field);
                  }
                })
            .toList();

    if (fields.isEmpty()) {
      return new TableStats().setColumns(Collections.<ColumnStats>emptyList()).setCount(0L);
    }

    final Iterable<StatColumn> statColumns =
        FluentIterable.from(fields)
            .transformAndConcat(
                new Function<RelDataTypeField, Iterable<? extends StatColumn>>() {
                  @Nullable
                  @Override
                  public Iterable<? extends StatColumn> apply(
                      @Nullable final RelDataTypeField field) {
                    return getStatColumnsPerField(field);
                  }
                });

    String pathString = table.getPath().getSchemaPath();
    // Append version context to dataset path if versioned
    if (VersionedDatasetId.isVersionedDatasetId(datasetId)) {
      final VersionedDatasetId versionedDatasetId;
      try {
        versionedDatasetId = VersionedDatasetId.fromString(datasetId);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(
            String.format("Unable to parse versionedDatasetId %s", datasetId), e);
      }
      pathString += " at " + versionedDatasetId.getVersionContext().toSql();
    }

    final String selection =
        Joiner.on(", ")
            .join(
                FluentIterable.from(statColumns)
                    .transform(
                        new Function<StatColumn, String>() {
                          @Override
                          public String apply(StatColumn input) {
                            return input.toString();
                          }
                        })
                    .append(SELECT_COUNT_STAR));

    final String sql = String.format("select %s from %s", selection, pathString);

    final SqlQuery query =
        SqlQuery.newBuilder()
            .setSql(sql)
            .addAllContext(Collections.<String>emptyList())
            .setUsername(SYSTEM_USERNAME)
            .build();

    final CompletionListener completionListener = new CompletionListener();
    final JobId jobId =
        jobsService
            .submitJob(
                SubmitJobRequest.newBuilder()
                    .setSqlQuery(query)
                    .setQueryType(JobsProtoUtil.toBuf(QueryType.UI_INTERNAL_PREVIEW))
                    .setVersionedDataset(
                        VersionedDatasetPath.newBuilder()
                            .addAllPath(NONE_PATH.getPathComponents())
                            .build())
                    .build(),
                completionListener)
            .getJobId();
    completionListener.awaitUnchecked();
    try (JobDataFragment data =
        JobDataClientUtils.getJobData(jobsService, bufferAllocator, jobId, 0, 1)) {
      final List<ColumnStats> columns =
          FluentIterable.from(fields)
              .transform(
                  new Function<RelDataTypeField, ColumnStats>() {
                    @Nullable
                    @Override
                    public ColumnStats apply(@Nullable final RelDataTypeField input) {
                      return buildColumn(data, input);
                    }
                  })
              .toList();

      Long count = getCount(data);

      return new TableStats().setColumns(columns).setCount(count);
    }
  }

  protected Iterable<StatColumn> getStatColumnsPerField(final RelDataTypeField field) {
    final RelDataTypeFamily family = field.getType().getFamily();
    Collection<StatType> dims = DIMENSIONS.get(family);
    if (dims.isEmpty()) {
      dims = DIMENSIONS.get(SqlTypeFamily.ANY);
    }

    return FluentIterable.from(dims)
        .transform(
            new Function<StatType, StatColumn>() {
              @Override
              public StatColumn apply(final StatType type) {
                return new StatColumn(type, field);
              }
            });
  }

  Long getCount(final JobDataFragment data) {
    return (Long) data.extractValue(COUNT_COLUMN, 0);
  }

  protected ColumnStats buildColumn(final JobDataFragment data, final RelDataTypeField field) {
    final Iterable<StatColumn> stats = getStatColumnsPerField(field);
    final RField rField = TypeUtils.fromCalciteField(field);
    final ColumnStats column = new ColumnStats().setField(rField);

    if (TypeUtils.isBoolean(rField)) {
      column.setCardinality(2L);
    }

    for (final StatColumn stat : stats) {
      final Object value = stat.toValue(data);
      if (value == null) {
        continue;
      }
      switch (stat.getType()) {
        case COUNT:
          column.setCount(Long.valueOf(value.toString()));
          break;
        case COUNT_DISTINCT:
        case COUNT_DISTINCT_DATE:
          column.setCardinality(Long.valueOf(value.toString()));
          break;
        case AVG:
          column.setAverageValue(Double.valueOf(value.toString()));
          break;
        case AVG_LEN:
          column.setAverageLength(Double.valueOf(value.toString()));
          break;
        case MAX:
          column.setMaxValue(Double.valueOf(value.toString()));
          break;
        case MAX_LEN:
          column.setMaxLength(Long.valueOf(value.toString()));
          break;
        case MIN:
          column.setMinValue(Double.valueOf(value.toString()));
          break;
        case MIN_LEN:
          column.setMinLength(Long.valueOf(value.toString()));
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("unsupported stat type: %s", stat.getType()));
      }
    }
    return column;
  }

  // helper structs

  private enum StatType {
    COUNT("count(%s)", "count_%s"),
    COUNT_DISTINCT("ndv(%s)", "count_distinct_%s"),
    COUNT_DISTINCT_DATE("ndv(cast(%s as date))", "count_distinct_%s"),
    MIN("min(%s)", "min_%s"),
    MAX("max(%s)", "max_%s"),
    AVG("avg(%s)", "avg_%s"),
    MIN_LEN("min(octet_length(%s))", "min_len_%s"),
    MAX_LEN("max(octet_length(%s))", "max_len_%s"),
    AVG_LEN("avg(octet_length(%s))", "avg_len_%s");
    private final String function;
    private final String alias;

    StatType(final String function, final String alias) {
      this.function = Preconditions.checkNotNull(function, "function pattern is required");
      this.alias = Preconditions.checkNotNull(alias, "alias pattern is required");
    }

    public String getAliasPattern() {
      return alias;
    }

    public String getFunctionPattern() {
      return function;
    }
  }

  /** javadoc */
  public static class TableStats {
    private List<ColumnStats> columns;
    private Long count;

    public List<ColumnStats> getColumns() {
      return columns;
    }

    public TableStats setColumns(List<ColumnStats> columns) {
      this.columns = columns;
      return this;
    }

    public Long getCount() {
      return count;
    }

    public TableStats setCount(Long count) {
      this.count = count;
      return this;
    }
  }

  /** javadoc */
  public static class RField {
    private String name;
    private String typeFamily;

    public String getName() {
      return name;
    }

    public RField setName(String name) {
      this.name = name;
      return this;
    }

    public String getTypeFamily() {
      return typeFamily;
    }

    public RField setTypeFamily(String typeFamily) {
      this.typeFamily = typeFamily;
      return this;
    }
  }

  /** javadoc */
  public static class ColumnStats {
    static final Long DEFAULT_CARDINALITY = -1L;
    static final Long DEFAULT_COUNT = -1L;
    static final Double DEFAULT_AVERAGE_LENGTH = -1.0d;
    static final Long DEFAULT_MIN_LENGTH = -1L;
    static final Long DEFAULT_MAX_LENGTH = -1L;
    static final Double DEFAULT_AVERAGE_VALUE = -1.0d;
    static final Double DEFAULT_MIN_VALUE = -1.0d;
    static final Double DEFAULT_MAX_VALUE = -1.0d;

    private RField field;
    private Long cardinality = DEFAULT_CARDINALITY;
    private Long count = DEFAULT_COUNT;
    private Double averageLength = DEFAULT_AVERAGE_LENGTH;
    private Long minLength = DEFAULT_MIN_LENGTH;
    private Long maxLength = DEFAULT_MAX_LENGTH;
    private Double averageValue = DEFAULT_AVERAGE_VALUE;
    private Double minValue = DEFAULT_MIN_VALUE;
    private Double maxValue = DEFAULT_MAX_VALUE;

    public RField getField() {
      return field;
    }

    public ColumnStats setField(RField field) {
      this.field = field;
      return this;
    }

    public Long getCardinality() {
      return cardinality;
    }

    public ColumnStats setCardinality(Long cardinality) {
      this.cardinality = cardinality;
      return this;
    }

    public Long getCount() {
      return count;
    }

    public ColumnStats setCount(Long count) {
      this.count = count;
      return this;
    }

    public Double getAverageLength() {
      return averageLength;
    }

    public ColumnStats setAverageLength(Double averageLength) {
      this.averageLength = averageLength;
      return this;
    }

    public Long getMinLength() {
      return minLength;
    }

    public ColumnStats setMinLength(Long minLength) {
      this.minLength = minLength;
      return this;
    }

    public Long getMaxLength() {
      return maxLength;
    }

    public ColumnStats setMaxLength(Long maxLength) {
      this.maxLength = maxLength;
      return this;
    }

    public Double getAverageValue() {
      return averageValue;
    }

    public ColumnStats setAverageValue(Double averageValue) {
      this.averageValue = averageValue;
      return this;
    }

    public Double getMinValue() {
      return minValue;
    }

    public ColumnStats setMinValue(Double minValue) {
      this.minValue = minValue;
      return this;
    }

    public Double getMaxValue() {
      return maxValue;
    }

    public ColumnStats setMaxValue(Double maxValue) {
      this.maxValue = maxValue;
      return this;
    }
  }

  private static class StatColumn {
    private static final String PATTERN = "%1$s as %2$s%3$s%2$s";
    private final StatType type;
    private final String alias;
    private final String call;

    public StatColumn(final StatType type, final RelDataTypeField field) {
      this.type = type;
      this.call =
          String.format(
              type.getFunctionPattern(), SqlUtils.QUOTE + field.getName() + SqlUtils.QUOTE);
      this.alias = String.format(type.getAliasPattern(), field.getName());
    }

    public StatType getType() {
      return type;
    }

    public String toSql() {
      return String.format(PATTERN, call, SqlUtils.QUOTE, alias);
    }

    public Object toValue(final JobDataFragment data) {
      return data.extractValue(alias, 0);
    }

    @Override
    public String toString() {
      return toSql();
    }
  }
}

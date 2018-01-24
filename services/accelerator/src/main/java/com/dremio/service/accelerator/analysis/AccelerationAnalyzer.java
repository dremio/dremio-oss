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
package com.dremio.service.accelerator.analysis;

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.utils.SqlUtils;
import com.dremio.service.accelerator.TypeUtils;
import com.dremio.service.accelerator.proto.ColumnStats;
import com.dremio.service.accelerator.proto.DatasetAnalysis;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

/**
 * Analyzes acceleration and generates statistics.
 */
public class AccelerationAnalyzer {
  private static final Logger logger = LoggerFactory.getLogger(AccelerationAnalyzer.class);
  private static final NamespaceKey NONE_PATH = new NamespaceKey(ImmutableList.of("__none"));

  private static final Multimap<RelDataTypeFamily, StatType> DIMENSIONS = HashMultimap.create();
  private static final Multimap<RelDataTypeFamily, StatType> DIMENSIONS_ALTERNATE = HashMultimap.create();

  static {
    DIMENSIONS.putAll(SqlTypeFamily.CHARACTER, ImmutableList.of(
        StatType.COUNT,
        StatType.COUNT_DISTINCT,
        StatType.MIN_LEN,
        StatType.MAX_LEN,
        StatType.AVG_LEN
    ));

    DIMENSIONS.putAll(SqlTypeFamily.NUMERIC, ImmutableList.of(
        StatType.COUNT,
        StatType.COUNT_DISTINCT,
        StatType.MIN,
        StatType.MAX,
        StatType.AVG
    ));

    DIMENSIONS_ALTERNATE.putAll(SqlTypeFamily.CHARACTER, ImmutableList.of(
      StatType.COUNT,
      StatType.COUNT_DISTINCT,
      StatType.MIN_LEN_ALTERNATE,
      StatType.MAX_LEN_ALTERNATE,
      StatType.AVG_LEN_ALTERNATE
    ));

    DIMENSIONS_ALTERNATE.putAll(SqlTypeFamily.NUMERIC, ImmutableList.of(
      StatType.COUNT,
      StatType.COUNT_DISTINCT,
      StatType.MIN,
      StatType.MAX,
      StatType.AVG
    ));
  }

  private final JobsService jobsService;
  private final Multimap<RelDataTypeFamily, StatType> dimensionsMap;


  public AccelerationAnalyzer(final JobsService jobsService, final boolean useAlternateAnalysis) {
    this.jobsService = Preconditions.checkNotNull(jobsService, "job service is required");
    dimensionsMap = useAlternateAnalysis ? DIMENSIONS_ALTERNATE : DIMENSIONS;
  }

  public AccelerationAnalysis analyze(final NamespaceKey path) {
    final RelNode plan = getPlan(path);

    final RelDataType rowType = plan.getRowType();
    final List<RelDataTypeField> fields = FluentIterable.from(rowType.getFieldList())
        .filter(new Predicate<RelDataTypeField>() {
          @Override
          public boolean apply(@Nullable final RelDataTypeField input) {
            final LayoutField field = TypeUtils.fromCalciteField(input);

            return TypeUtils.isBoolean(field)
                || TypeUtils.isTemporal(field)
                || TypeUtils.isText(field)
                || TypeUtils.isNumeric(field);
          }
        })
        .toList();

    if (fields.isEmpty()) {
      return new AccelerationAnalysis(new DatasetAnalysis(), plan);
    }

    final Iterable<StatColumn> statColumns = FluentIterable.from(fields)
        .transformAndConcat(new Function<RelDataTypeField, Iterable<? extends StatColumn>>() {
          @Nullable
          @Override
          public Iterable<? extends StatColumn> apply(@Nullable final RelDataTypeField field) {
            return getStatColumnsPerField(field);
          }
        });

    final String pathString = path.getSchemaPath();

    final String measures = Joiner.on(", ").join(statColumns);
    final String sql = String.format("select %s from %s", measures, pathString);

    final SqlQuery query = new SqlQuery(sql, SYSTEM_USERNAME);

    final Job job = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(query)
        .setQueryType(QueryType.UI_INTERNAL_PREVIEW)
        .setDatasetPath(NONE_PATH)
        .setDatasetVersion(DatasetVersion.NONE)
        .build(), new NoOpJobStatusListener() {
      @Override
      public void jobFailed(final Exception e) {
        logger.warn("query analysis failed for {}", query, e);
      }
    });

    // trunc blocks until job completion
    final JobDataFragment data = job.getData().truncate(1);

    final List<ColumnStats> columns = FluentIterable.from(fields)
        .transform(new Function<RelDataTypeField, ColumnStats>() {
          @Nullable
          @Override
          public ColumnStats apply(@Nullable final RelDataTypeField input) {
            return buildColumn(data, input);
          }
        })
        .toList();

    final DatasetAnalysis datasetAnalysis = new DatasetAnalysis()
        .setColumnList(columns);

    return new AccelerationAnalysis(datasetAnalysis, plan);
  }

  public RelNode getPlan(final NamespaceKey path) {
    final String pathString = path.getSchemaPath();
    final String sql = String.format("explain plan for select * from %s", pathString);

    final SqlQuery query = new SqlQuery(sql, SYSTEM_USERNAME);
    final RelNode[] planHolder = new RelNode[1];
    final Job job = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(query)
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(NONE_PATH)
        .setDatasetVersion(DatasetVersion.NONE)
        .build(), new NoOpJobStatusListener() {
      @Override
      public void metadataCollected(final QueryMetadata metadata) {
        planHolder[0] = metadata.getSerializableLogicalPlan().get();
      }

      @Override
      public void jobFailed(final Exception e) {
        logger.warn("query analysis failed for {}", sql, e);
      }
    });

    // trunc blocks until job completion
    job.getData().truncate(1);
    return Preconditions.checkNotNull(planHolder[0], "plan is required");
  }


  protected Iterable<StatColumn> getStatColumnsPerField(final RelDataTypeField field) {
    final RelDataTypeFamily family = field.getType().getFamily();
    final Collection<StatType> dims = dimensionsMap.get(family);
    if (dims.isEmpty()) {
      return ImmutableList.of();
    }

    return FluentIterable.from(dims)
        .transform(new Function<StatType, StatColumn>() {
          @Nullable
          @Override
          public StatColumn apply(@Nullable final StatType type) {
            return new StatColumn(type, field);
          }
        });
  }

  protected ColumnStats buildColumn(final JobDataFragment data, final RelDataTypeField field) {
    final Iterable<StatColumn> stats = getStatColumnsPerField(field);
    final LayoutField layoutField = TypeUtils.fromCalciteField(field);
    final ColumnStats column = new ColumnStats()
        .setField(layoutField);

    if (TypeUtils.isBoolean(layoutField)) {
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
          column.setCardinality(Long.valueOf(value.toString()));
          break;
        case AVG:
          column.setAverageValue(Double.valueOf(value.toString()));
          break;
        case AVG_LEN:
        case AVG_LEN_ALTERNATE:
          column.setAverageLength(Double.valueOf(value.toString()));
          break;
        case MAX:
          column.setMaxValue(Double.valueOf(value.toString()));
          break;
        case MAX_LEN:
        case MAX_LEN_ALTERNATE:
          column.setMaxLength(Long.valueOf(value.toString()));
          break;
        case MIN:
          column.setMinValue(Double.valueOf(value.toString()));
          break;
        case MIN_LEN:
        case MIN_LEN_ALTERNATE:
          column.setMinLength(Long.valueOf(value.toString()));
          break;
        default:
          throw new UnsupportedOperationException(String.format("unsupported stat type: %s", stat.getType()));
      }
    }
    return column;
  }


  // helper structs

  private enum StatType {
    COUNT("count(%s)", "count_%s"),
    COUNT_DISTINCT("ndv(%s)", "count_distinct_%s"),
    MIN("min(%s)", "min_%s"),
    MAX("max(%s)", "max_%s"),
    AVG("avg(%s)", "avg_%s"),
    MIN_LEN("min(length(%s))", "min_len_%s"),
    MAX_LEN("max(length(%s))", "max_len_%s"),
    AVG_LEN("avg(length(%s))", "avg_len_%s"),
    MIN_LEN_ALTERNATE("min(octet_length(%s))", "min_len_%s"),
    MAX_LEN_ALTERNATE("max(octet_length(%s))", "max_len_%s"),
    AVG_LEN_ALTERNATE("avg(octet_length(%s))", "avg_len_%s");
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

  private static class StatColumn {
    private static final String PATTERN = "%1$s as %2$s%3$s%2$s";
    private final StatType type;
    private final String alias;
    private final String call;

    public StatColumn(final StatType type, final RelDataTypeField field) {
      this.type = type;
      this.call = String.format(type.getFunctionPattern(), SqlUtils.QUOTE + field.getName() + SqlUtils.QUOTE);
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

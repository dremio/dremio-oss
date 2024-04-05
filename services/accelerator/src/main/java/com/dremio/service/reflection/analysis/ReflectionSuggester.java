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

import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.reflection.ReflectionValidator;
import com.dremio.service.reflection.analysis.ReflectionAnalyzer.ColumnStats;
import com.dremio.service.reflection.analysis.ReflectionAnalyzer.RField;
import com.dremio.service.reflection.analysis.ReflectionAnalyzer.TableStats;
import com.dremio.service.reflection.proto.DimensionGranularity;
import com.dremio.service.reflection.proto.ReflectionDetails;
import com.dremio.service.reflection.proto.ReflectionDimensionField;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionMeasureField;
import com.dremio.service.reflection.proto.ReflectionType;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Suggest reflections */
public class ReflectionSuggester {
  private static final Logger logger = LoggerFactory.getLogger(ReflectionSuggester.class);

  private static final Comparator<ColumnStats> COLUMN_RANKING =
      new Comparator<ColumnStats>() {
        @Override
        public int compare(final ColumnStats left, final ColumnStats right) {
          return Long.compare(
              Optional.ofNullable(left.getCardinality()).orElse(Long.MAX_VALUE),
              Optional.ofNullable(right.getCardinality()).orElse(Long.MAX_VALUE));
        }
      };

  private static final Function<ColumnStats, ReflectionField> TO_REFLECTION_FIELD =
      new Function<ColumnStats, ReflectionField>() {
        @Override
        public ReflectionField apply(final ColumnStats columnStats) {
          return new ReflectionField(columnStats.getField().getName());
        }
      };

  private static final Predicate<RField> MEASURE_TYPE_FILTER =
      new Predicate<RField>() {
        @Override
        public boolean apply(@Nullable final RField input) {
          return TypeUtils.isNumeric(input);
        }
      };

  private static final Predicate<ColumnStats> DIMENSION_TYPE_FILTER =
      new Predicate<ColumnStats>() {
        @Override
        public boolean apply(@Nullable final ColumnStats columnStats) {
          return columnStats.getMaxLength() <= MAX_DIMENSION_FIELD_LENGTH
              && !TypeUtils.isComplex(columnStats.getField());
        }
      };

  // ratio of dimension fields to all fields
  private static final double DIMENSION_FIELDS_RATIO = 1;
  // maximum number of measure fields to discover
  private static final int MAX_MEASURE_FIELDS = 30;
  // ratio of measure fields to all fields
  private static final double MEASURE_FIELDS_RATIO = .9;
  // max field length to be considered as dimension or measure column
  private static final int MAX_DIMENSION_FIELD_LENGTH = 50;
  // Setup an upper limit for cartesian product 2 pow 31 = 1 TB assuming 500  bytes for a row
  private static final long CARTESIAN_CARDINALITY_UPPER_LIMIT = 2L << 30;

  private final DatasetConfig datasetConfig;
  private final List<ColumnStats> columnStats;
  private final Long count;

  public ReflectionSuggester(DatasetConfig datasetConfig, TableStats tableStats) {
    this.datasetConfig = datasetConfig;
    this.columnStats = tableStats.getColumns();
    this.count = tableStats.getCount();
  }

  public List<ReflectionGoal> getReflectionGoals() {
    List<ReflectionGoal> rawGoals =
        Lists.transform(
            Ord.zip(getRawReflections()),
            new Function<Ord<ReflectionDetails>, ReflectionGoal>() {
              @Override
              public ReflectionGoal apply(Ord<ReflectionDetails> reflectionDetails) {
                return new ReflectionGoal()
                    .setName(
                        String.format(
                            "AUTO_%s_RAW_%d",
                            SqlUtils.quotedCompound(datasetConfig.getFullPathList()),
                            reflectionDetails.i))
                    .setDetails(reflectionDetails.e)
                    .setType(ReflectionType.RAW);
              }
            });
    List<ReflectionGoal> aggGoals =
        Lists.transform(
            Ord.zip(getAggReflections()),
            new Function<Ord<ReflectionDetails>, ReflectionGoal>() {
              @Override
              public ReflectionGoal apply(Ord<ReflectionDetails> reflectionDetails) {
                return new ReflectionGoal()
                    .setName(
                        String.format(
                            "AUTO_%s_AGG_%d",
                            SqlUtils.quotedCompound(datasetConfig.getFullPathList()),
                            reflectionDetails.i))
                    .setDetails(reflectionDetails.e)
                    .setType(ReflectionType.AGGREGATION);
              }
            });

    return FluentIterable.from(rawGoals).append(aggGoals).toList();
  }

  private List<ReflectionDetails> getAggReflections() {
    List<ColumnStats> columns = columnStats;
    if (columns.isEmpty()) {
      return Collections.emptyList();
    }

    final int columnCount = columns.size();
    final int measureLimit =
        Math.min(MAX_MEASURE_FIELDS, Math.max(1, (int) (columnCount * MEASURE_FIELDS_RATIO)));

    AnalysisSummary analysisSummary = AnalysisSummary.of(columns, count);

    final Map<String, ViewFieldType> schema =
        FluentIterable.from(
                Optional.ofNullable(ViewFieldsHelper.getViewFields(datasetConfig))
                    .orElse(Collections.emptyList()))
            .uniqueIndex(
                new Function<ViewFieldType, String>() {
                  @Override
                  public String apply(final ViewFieldType input) {
                    return input.getName();
                  }
                });

    // create a ranking based on stats
    final List<ColumnStats> candidates = FluentIterable.from(columns).toSortedList(COLUMN_RANKING);

    final List<ColumnStats> dimension =
        FluentIterable.from(candidates)
            .filter(
                new Predicate<ColumnStats>() {
                  @Override
                  public boolean apply(final ColumnStats columnStats) {
                    return DIMENSION_TYPE_FILTER.apply(columnStats);
                  }
                })
            .filter(
                new Predicate<ColumnStats>() {
                  @Override
                  public boolean apply(final ColumnStats columnStats) {

                    final RField columnRField = columnStats.getField();
                    final String name = columnRField.getName();
                    final ViewFieldType fieldType = schema.get(name);
                    final SqlTypeName sqlTypeName = SqlTypeName.get(fieldType.getType());
                    boolean result = true;

                    /*
                     * DX-7524: Some types from NUMERIC family (DECIMAL, FLOATs and not INTs) should never be
                     * considered as DIMENSIONS. These should always be MEASURES. For other NUMERIC
                     * types (INTEGER, TINYINT, SMALLINT, BIGINT), the existing logic of checking
                     * cardinality and cartesian product remains unchanged.
                     */
                    if (TypeUtils.isNumeric(columnRField)) {
                      switch (sqlTypeName) {
                        case DECIMAL:
                        case FLOAT:
                        case REAL:
                        case DOUBLE:
                          result = false;
                          break;
                        default:
                          result = true;
                          break;
                      }
                    }

                    return result;
                  }
                })
            .toList();

    final List<ColumnStats> measure =
        FluentIterable.from(Lists.reverse(candidates))
            .filter(
                new Predicate<ColumnStats>() {
                  @Override
                  public boolean apply(final ColumnStats columnStats) {
                    return MEASURE_TYPE_FILTER.apply(columnStats.getField());
                  }
                })
            .limit(measureLimit)
            .toList();

    // generate aggregation suggestions
    Optional<AggregationDescriptor> aggregation =
        generate(dimension, measure, analysisSummary.getCount());

    if (aggregation.isPresent()) {
      return FluentIterable.from(Arrays.asList(aggregation.get()))
          .transform(
              new Function<AggregationDescriptor, ReflectionDetails>() {
                @Override
                public ReflectionDetails apply(final AggregationDescriptor input) {
                  return new ReflectionDetails()
                      .setDimensionFieldList(toReflectionDimensionFields(input.getDimensions()))
                      .setMeasureFieldList(
                          input.getMeasures().stream()
                              .map(
                                  stats ->
                                      new ReflectionMeasureField(stats.getField().getName())
                                          .setMeasureTypeList(
                                              ReflectionValidator.getDefaultMeasures(
                                                  stats.getField().getTypeFamily())))
                              .collect(Collectors.toList()));
                }
              })
          .toList();
    }
    return Collections.emptyList();
  }

  /**
   * Suggests raw reflection.
   *
   * <p>Current implementation is simply a pass through.
   */
  private List<ReflectionDetails> getRawReflections() {
    return ImmutableList.of(
        new ReflectionDetails()
            .setDisplayFieldList(
                Lists.transform(
                    columnStats,
                    new Function<ColumnStats, ReflectionField>() {
                      @Override
                      public ReflectionField apply(ColumnStats column) {
                        return new ReflectionField(column.getField().getName());
                      }
                    })));
  }

  /**
   * Generates a single aggregation.
   *
   * <p>This algorithm is heuristic based so there no guarantees as to find the optimal solution.
   * The resulting plan should satisfy the following:
   *
   * <p>(1) there is at least a dimension column (2) cardinality of each dimension should be less
   * than square root of max cardinality(except if there is only one dimension) (3) cartesian
   * product should be less than 2 Billion (assuming 500 bytes for each row, this is 1 TB)
   */
  protected Optional<AggregationDescriptor> generate(
      final List<ColumnStats> dimensions, final List<ColumnStats> measures, Long count) {
    final List<ColumnStats> dimensionFields = Lists.newArrayList();

    if (!dimensions.isEmpty()) {
      // add the first one anyway
      dimensionFields.add(dimensions.get(0));
      long currentCardinalityProduct = dimensions.get(0).getCardinality();

      double cardinalityLimit = (Optional.ofNullable(count).orElse(100_000L)) * .01;

      for (int i = 1; i < dimensions.size(); i++) {
        final ColumnStats field = dimensions.get(i);
        long newCardinalityProduct = currentCardinalityProduct * field.getCardinality();

        if (field.getCardinality() <= cardinalityLimit
            && newCardinalityProduct <= CARTESIAN_CARDINALITY_UPPER_LIMIT) {
          dimensionFields.add(field);
          currentCardinalityProduct = newCardinalityProduct;
        }
      }

      final List<ColumnStats> measureFields =
          FluentIterable.from(measures)
              .filter(
                  new Predicate<ColumnStats>() {
                    @Override
                    public boolean apply(final ColumnStats columnStats) {
                      return !dimensionFields.contains(columnStats);
                    }
                  })
              .toList();
      return Optional.of(
          new AggregationDescriptor(ImmutableList.copyOf(dimensionFields), measureFields));
    }
    return Optional.empty();
  }

  private static List<ReflectionDimensionField> toReflectionDimensionFields(
      final Iterable<ColumnStats> columns) {
    return FluentIterable.from(columns)
        .transform(
            new Function<ColumnStats, ReflectionDimensionField>() {
              @Nullable
              @Override
              public ReflectionDimensionField apply(final ColumnStats columnStats) {
                return new ReflectionDimensionField()
                    .setName(columnStats.getField().getName())
                    .setGranularity(DimensionGranularity.DATE);
              }
            })
        .toList();
  }

  static List<ReflectionField> toReflectionFields(final Iterable<ColumnStats> columns) {
    return FluentIterable.from(columns).transform(TO_REFLECTION_FIELD).toList();
  }

  // utility classes

  /**
   * Returns a truncated view root that is the bottom part of the plan after splitting input view
   * from aggregation.
   */
  protected static class AggregationDescriptor {
    private final List<ColumnStats> dimensions;
    private final List<ColumnStats> measures;

    public AggregationDescriptor(
        final List<ColumnStats> dimensions, final List<ColumnStats> measures) {
      this.dimensions = Preconditions.checkNotNull(dimensions, "dimensions are required");
      this.measures = Preconditions.checkNotNull(measures, "measures are required");
      Preconditions.checkArgument(!dimensions.isEmpty(), "dimensions cannot be empty");
    }

    public List<ColumnStats> getMeasures() {
      return measures;
    }

    public List<ColumnStats> getDimensions() {
      return dimensions;
    }
  }

  /** Summary of dataset analysis */
  private static final class AnalysisSummary {
    private final List<ColumnStats> analysis;
    private final long count;

    public AnalysisSummary(final List<ColumnStats> analysis, final long count) {
      this.analysis = analysis;
      this.count = count;
    }

    public List<ColumnStats> getAnalysis() {
      return analysis;
    }

    public long getCount() {
      return count;
    }

    public static AnalysisSummary of(final List<ColumnStats> columnList, Long count) {
      final List<ColumnStats> columns = AccelerationUtils.selfOrEmpty(columnList);

      if (columns.isEmpty()) {
        return new AnalysisSummary(columnList, count);
      }

      return new AnalysisSummary(columnList, count);
    }
  }
}

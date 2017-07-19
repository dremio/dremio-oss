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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.TypeUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.ColumnStats;
import com.dremio.service.accelerator.proto.DatasetAnalysis;
import com.dremio.service.accelerator.proto.DimensionGranularity;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.LogicalAggregation;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Suggest accelerations
 */
public class AccelerationSuggestor {
  private static final String AUTO_SUGGESTED_LAYOUT_NAME = "";

  private static final Logger logger = LoggerFactory.getLogger(AccelerationSuggestor.class);

  static final Function<Column, LayoutField> TO_LAYOUT_FIELD = new Function<Column, LayoutField>() {
    @Nullable
    @Override
    public LayoutField apply(@Nullable final Column input) {
      return input.getStats().getField();
    }
  };

  private static final Predicate<LayoutField> MEASURE_TYPE_FILTER = new Predicate<LayoutField>() {
    @Override
    public boolean apply(@Nullable final LayoutField input) {
      return TypeUtils.isNumeric(input);
    }
  };

  private static final Predicate<Column> DIMENSION_TYPE_FILTER = new Predicate<Column>() {
    @Override
    public boolean apply(@Nullable final Column input) {
      return input.getStats().getMaxLength() <= MAX_DIMENSION_FIELD_LENGTH && !TypeUtils.isComplex(input.getStats().getField());
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
  //Setup an upper limit for cartesian product 2 pow 31 = 1 TB assuming 500  bytes for a row
  private static final long CARTESIAN_CARDINALITY_UPPER_LIMIT = 2L << 30;

  // maximum number of dimension fields to discover
  private int maxDimensionFields;

  public AccelerationSuggestor(int maxDimensionFields) {
    this.maxDimensionFields = maxDimensionFields;
  }

  /**
   * Suggest aggregate accelerations.
   *
   * @param acceleration
   * @return
   */
  public void writeAggregationLayouts(final Acceleration acceleration) {
    final AccelerationContext context = acceleration.getContext();
    final DatasetAnalysis analysis = context.getAnalysis();
    final List<ColumnStats> columns = AccelerationUtils.selfOrEmpty(analysis.getColumnList());
    if (columns.isEmpty()) {
      return;
    }

    final LayoutContainer aggLayouts = Optional.fromNullable(acceleration.getAggregationLayouts())
        .or(new LayoutContainer().setType(LayoutType.AGGREGATION));
    acceleration.setAggregationLayouts(aggLayouts);

    final int columnCount = columns.size();
    final int dimensionLimit = Math.min(maxDimensionFields, Math.max(1, (int) (columnCount * DIMENSION_FIELDS_RATIO)));
    final int measureLimit = Math.min(MAX_MEASURE_FIELDS, Math.max(1, (int) (columnCount * MEASURE_FIELDS_RATIO)));

    AnalysisSummary analysisSummary = AnalysisSummary.of(analysis);

    /* DX-7524: get the schema definition to examine the SQL type */
    final Map<String, ViewFieldType> schema = FluentIterable
      .from(context.getDatasetSchema().getFieldList())
      .uniqueIndex(new Function<ViewFieldType, String>() {
        @Nullable
        @Override
        public String apply(@Nullable final ViewFieldType input) {
          return input.getName();
        }
      });

    // create a ranking based on stats
    final ColumnRanking ranking = new ColumnRanking(analysisSummary);
    final List<Column> candidates = FluentIterable
        .from(analysis.getColumnList())
        .transform(new Function<ColumnStats, Column>() {
          @Nullable
          @Override
          public Column apply(@Nullable final ColumnStats input) {
            return new Column(input);
          }
        })
        .toSortedList(ranking);

    final List<Column> dimension = FluentIterable
        .from(candidates)
        .filter(new Predicate<Column>() {
          @Override
          public boolean apply(@Nullable final Column input) {
            return DIMENSION_TYPE_FILTER.apply(input);
          }
        })
        .filter(new Predicate<Column>() {
        @Override
        public boolean apply(@Nullable final Column input) {

           final LayoutField columnLayoutField = input.getStats().getField();
           final String name = columnLayoutField.getName();
           final ViewFieldType fieldType = schema.get(name);
           final SqlTypeName sqlTypeName = SqlTypeName.get(fieldType.getType());
           boolean result = true;

           /*
            * DX-7524: Some types from NUMERIC family (DECIMAL, FLOATs and not INTs) should never be
            * considered as DIMENSIONS. These should always be MEASURES. For other NUMERIC
            * types (INTEGER, TINYINT, SMALLINT, BIGINT), the existing logic of checking
            * cardinality and cartesian product remains unchanged.
            */
           if(TypeUtils.isNumeric(columnLayoutField)) {
            switch(sqlTypeName) {
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
        .limit(dimensionLimit)
        .toList();

    final List<Column> measure = FluentIterable
        .from(Lists.reverse(candidates))
        .filter(new Predicate<Column>() {
          @Override
          public boolean apply(@Nullable final Column input) {
            return MEASURE_TYPE_FILTER.apply(input.getStats().getField());
          }
        })
        .limit(measureLimit)
        .toList();

    // generate aggregation suggestions
    Optional<AggregationDescriptor> aggregation = generate(dimension, measure, analysisSummary.getMaxCardinality());

    if (aggregation.isPresent()) {
      final List<Layout> layouts = FluentIterable
          .from(Arrays.asList(aggregation.get()))
          .transform(new Function<AggregationDescriptor, Layout>() {
            @Nullable
            @Override
            public Layout apply(@Nullable final AggregationDescriptor input) {
              final Layout layout = new Layout()
                  .setId(AccelerationUtils.newRandomId())
                  .setName(AUTO_SUGGESTED_LAYOUT_NAME)
                  .setVersion(1)
                  .setLayoutType(LayoutType.AGGREGATION)
                  .setDetails(
                      new LayoutDetails()
                      .setDimensionFieldList(toLayoutDimensionFields(input.getDimensions()))
                      .setMeasureFieldList(toLayoutFields(input.getMeasures()))
                      );
              return layout;
            }
          })
          .toList();

      aggLayouts.setLayoutList(layouts);

      context.setLogicalAggregation(
          new LogicalAggregation()
          .setDimensionList(toLayoutFields(aggregation.get().getDimensions()))
          .setMeasureList(toLayoutFields(aggregation.get().getMeasures()))
          );
    }
  }

  /**
   * Suggests raw accelerations.
   * <p>
   * Current implementation is simply a pass through.
   *
   * @param acceleration
   * @return
   */
  public void writeRawLayouts(final Acceleration acceleration) {
    final LayoutContainer rawLayouts = Optional.fromNullable(acceleration.getRawLayouts())
        .or(new LayoutContainer().setType(LayoutType.RAW));
    acceleration.setRawLayouts(rawLayouts);

    final RowType datasetSchema = acceleration.getContext().getDatasetSchema();

    final Layout directLayout = new Layout()
        .setId(AccelerationUtils.newRandomId())
        .setLayoutType(LayoutType.RAW)
        .setName(AUTO_SUGGESTED_LAYOUT_NAME)
        .setVersion(1)
        .setDetails(
            new LayoutDetails()
                .setDisplayFieldList(
                    FluentIterable
                        .from(datasetSchema.getFieldList())
                        .transform(new Function<ViewFieldType, LayoutField>() {
                          @Nullable
                          @Override
                          public LayoutField apply(@Nullable final ViewFieldType input) {
                              return new LayoutField()
                                .setName(input.getName())
                                .setTypeFamily(input.getTypeFamily());
                          }
                        })
                        .toList()
                )
        );

    rawLayouts.setLayoutList(ImmutableList.of(directLayout));
  }

  /**
   * Generates a single aggregation.
   * <p>
   * This algorithm is heuristic based so there no guarantees as to find the optimal solution. The resulting plan
   * should satisfy the following:
   * <p>
   * (1) there is at least a dimension column
   * (2) cardinality of each dimension should be less than square root of max cardinality(except if there is only one dimension)
   * (3) cartesian product should be less than 2 Billion (assuming 500 bytes for each row, this is 1 TB)
   */
  protected Optional<AggregationDescriptor> generate(final List<Column> dimensions,
      final List<Column> measures, long maxCardinality) {
    final List<Column> dimensionFields = Lists.newArrayList();

    if (!dimensions.isEmpty()) {
      //add the first one anyway
      dimensionFields.add(dimensions.get(0));
      long currentCardinalityProduct = dimensions.get(0).getStats().getCardinality();

      double cardinalityLimit  = Math.sqrt(maxCardinality);

      for (int i = 1, n = dimensions.size(); i < n; i++) {
        final Column field = dimensions.get(i);
        long newCardinalityProduct = currentCardinalityProduct * field.getStats().getCardinality();

        if (field.getStats().getCardinality() <= cardinalityLimit && newCardinalityProduct <= CARTESIAN_CARDINALITY_UPPER_LIMIT) {
          dimensionFields.add(field);
          currentCardinalityProduct = newCardinalityProduct;
        }
      }

      final List<Column> measureFields = FluentIterable
          .from(measures)
          .filter(new Predicate<Column>() {
            @Override
            public boolean apply(@Nullable final Column input) {
              return !dimensionFields.contains(input);
            }
          }).toList();
      return Optional.of(new  AggregationDescriptor(ImmutableList.copyOf(dimensionFields), measureFields));
    }
    return Optional.absent();
  }

  private static List<LayoutDimensionField> toLayoutDimensionFields(final Iterable<Column> columns) {
    return FluentIterable.from(columns)
        .transform(new Function<Column, LayoutDimensionField>() {
          @Nullable
          @Override
          public LayoutDimensionField apply(@Nullable final Column input) {
            return new LayoutDimensionField()
                .setName(input.getStats().getField().getName())
                .setTypeFamily(input.getStats().getField().getTypeFamily())
                .setGranularity(DimensionGranularity.DATE);
          }
        })
        .toList();
  }

  static List<LayoutField> toLayoutFields(final Iterable<Column> columns) {
    return FluentIterable.from(columns)
        .transform(TO_LAYOUT_FIELD)
        .toList();
  }

  // utility classes

  /**
   * Returns a truncated view root that is the bottom part of the plan after splitting input view from aggregation.
   */
  protected static class AggregationDescriptor {
    private final List<Column> dimensions;
    private final List<Column> measures;

    public AggregationDescriptor(final List<Column> dimensions, final List<Column> measures) {
      this.dimensions = Preconditions.checkNotNull(dimensions, "dimensions are required");
      this.measures = Preconditions.checkNotNull(measures, "measures are required");
      Preconditions.checkArgument(!dimensions.isEmpty(), "dimensions cannot be empty");
    }

    public List<Column> getMeasures() {
      return measures;
    }

    public List<Column> getDimensions() {
      return dimensions;
    }
  }

  protected static final class ColumnRanking implements Comparator<Column> {
    private final AnalysisSummary analysis;

    public ColumnRanking(final AnalysisSummary analysis) {
      this.analysis = analysis;
    }

    @Override
    public int compare(final Column left, final Column right) {
      return Double.compare(left.computeRank(analysis), right.computeRank(analysis));
    }
  }

  /**
   * Summary of dataset analysis
   */
  private static final class AnalysisSummary {
    private final DatasetAnalysis analysis;
    private final long minCardinality;
    private final long maxCardinality;
    private final double avgCardinality;

    public AnalysisSummary(final DatasetAnalysis analysis, final long minCardinality,
                           final long maxCardinality, final double avgCardinality) {
      this.analysis = analysis;
      this.minCardinality = minCardinality;
      this.maxCardinality = maxCardinality;
      this.avgCardinality = avgCardinality;
    }

    public DatasetAnalysis getAnalysis() {
      return analysis;
    }

    public long getMinCardinality() {
      return minCardinality;
    }

    public long getMaxCardinality() {
      return maxCardinality;
    }

    public double getAvgCardinality() {
      return avgCardinality;
    }

    public static AnalysisSummary of(final DatasetAnalysis analysis) {
      final List<ColumnStats> columns = AccelerationUtils.selfOrEmpty(analysis.getColumnList());
      long maxCardinality = 0;
      long minCardinality = 0;
      double avgCardinality = 0;
      long cardinalitySum = 0;

      if (columns.isEmpty()) {
        return new AnalysisSummary(analysis, minCardinality, maxCardinality, avgCardinality);
      }

      maxCardinality = columns.get(0).getCardinality();
      minCardinality = maxCardinality;
      cardinalitySum = maxCardinality;

      for (int i = 1, n = columns.size(); i < n; i++) {
        long current = columns.get(i).getCardinality();
        maxCardinality = Math.max(maxCardinality, current);
        minCardinality = Math.min(minCardinality, current);
        cardinalitySum += current;
      }
      avgCardinality = cardinalitySum/columns.size();

      return new AnalysisSummary(analysis, minCardinality, maxCardinality, avgCardinality);
    }
  }

  /**
   * A column that can compute its rank given stats.
   */
  static final class Column {
    private final ColumnStats stats;

    public Column(final ColumnStats stats) {
      this.stats = stats;
    }

    public ColumnStats getStats() {
      return stats;
    }

    public double computeRank(final AnalysisSummary analysis) {
      final long cardinality = stats.getCardinality();
      final double maxCardinality = analysis.getMaxCardinality();

      return (40.0 * cardinality) / maxCardinality + 60 * getRankByType(stats.getField());
    }

    protected double getRankByType(final LayoutField field) {
      if (TypeUtils.isTemporal(field)) {
        return 0;
      }

      if (TypeUtils.isText(field)) {
        return .50;
      }

      if (TypeUtils.isBoolean(field)) {
        return .85;
      }

      return 1;
    }

    @Override
    public String toString() {
      return stats.toString();
    }
  }
}

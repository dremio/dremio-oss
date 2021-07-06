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
package com.dremio.service.statistics;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.google.common.collect.Range;
import com.tdunning.math.stats.TDigest;


/**
 * Histogram
 */
public class HistogramImpl implements StatisticsService.Histogram {
  private static final Logger logger = LoggerFactory.getLogger(HistogramImpl.class);
  private final TDigest tDigest;

  public HistogramImpl(ByteBuffer buff) {
    this.tDigest = com.tdunning.math.stats.MergingDigest.fromBytes(buff);
  }

  public boolean isTDigestSet() {
    return tDigest != null;
  }

  public double quantile(double q) {
    return tDigest.quantile(q);
  }

  /**
   * Estimate the selectivity of a filter which may contain several range predicates and in the general case is of
   * type: col op value1 AND col op value2 AND col op value3 ...
   * <p>
   * e.g a > 10 AND a < 50 AND a >= 20 AND a <= 70 ...
   * NOTE: 5 > a etc is ignored as this assume filters have been canonicalized by predicate push down logic
   * </p>
   * Even though in most cases it will have either 1 or 2 range conditions, we still have to handle the general case
   */
  @Override
  public Double estimatedRangeSelectivity(final RexNode columnFilter) {
    List<RexNode> filterList = RelOptUtil.conjunctions(columnFilter);
    List<RexNode> unknownFilterList = new ArrayList<RexNode>();
    Range<Double> valuesRange = getValuesRange(filterList, unknownFilterList);
    // unknown counter is a count of filter predicates whose bucket ranges cannot be
    // determined from the histogram; this may happen for instance when there is an expression or
    // function involved..e.g  col > CAST('10' as INT)
    int unknown = unknownFilterList.size();
    // for each 'unknown' range filter selectivity, use a default of 0.5 (matches Calcite)
    double scaleFactor = Math.pow(0.5, unknown);
    if (valuesRange.hasLowerBound() || valuesRange.hasUpperBound()) {
      Double lowValue = (valuesRange.hasLowerBound()) ? valuesRange.lowerEndpoint() : null;
      Double highValue = (valuesRange.hasUpperBound()) ? valuesRange.upperEndpoint() : null;
      if (highValue != null && lowValue != null) {
        return scaleFactor * (tDigest.cdf(highValue) - tDigest.cdf(lowValue));
      } else if (highValue != null) {
        return scaleFactor * (tDigest.cdf(highValue));
      } else {
        return scaleFactor * (1 - tDigest.cdf(lowValue));
      }
    }

    return null;
  }

  private Range<Double> getValuesRange(List<RexNode> filterList, List<RexNode> unkownFilterList) {
    Range<Double> currentRange = Range.all();
    for (RexNode filter : filterList) {
      if (filter instanceof RexCall) {
        Double value = getLiteralValue((RexCall) filter);
        if (value == null) {
          unkownFilterList.add(filter);
        } else {
          // get the operator
          SqlOperator op = ((RexCall) filter).getOperator();
          Range<Double> range;
          switch (op.getKind()) {
            case GREATER_THAN:
              range = Range.greaterThan(value);
              currentRange = currentRange.intersection(range);
              break;
            case GREATER_THAN_OR_EQUAL:
              range = Range.atLeast(value);
              currentRange = currentRange.intersection(range);
              break;
            case LESS_THAN:
              range = Range.lessThan(value);
              currentRange = currentRange.intersection(range);
              break;
            case LESS_THAN_OR_EQUAL:
              range = Range.atMost(value);
              currentRange = currentRange.intersection(range);
              break;
            default:
              break;
          }
        }
      }
    }
    return currentRange;
  }

  private Double getLiteralValue(final RexCall filter) {
    Double value = null;
    List<RexNode> operands = filter.getOperands();
    if (operands.size() == 2) {
      RexLiteral l = null;
      if (operands.get(1) instanceof RexLiteral) {
        l = ((RexLiteral) operands.get(1));
      } else if (operands.get(0) instanceof RexLiteral) {
        logger.warn(String.format("Filter %s ignored as it has not yet been canonicalize by predicate push down logic, which will rewrite it to the first parameter of the operand ( 5 > a ) rewritten to ( a < 5)",filter.toString()));
        return null;
      }
      if (l == null) {
        return null;
      }
      switch (l.getTypeName()) {
        case DATE:
        case TIMESTAMP:
        case TIME:
          value = (double) ((java.util.Calendar) l.getValue()).getTimeInMillis();
          return value;
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
          value = l.getValueAs(Double.class);
          return value;
        default:
          break;
      }
    }

    return null;
  }
}

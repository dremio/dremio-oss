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

import com.dremio.common.expression.ValueExpressions;
import com.dremio.exec.expr.fn.ItemsSketch.ItemsSketchFunctions;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.google.common.collect.Range;
import com.tdunning.math.stats.TDigest;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Histogram */
public class HistogramImpl implements StatisticsService.Histogram {
  private static final Logger logger = LoggerFactory.getLogger(HistogramImpl.class);
  private final TDigest tDigest;
  private final ItemsSketch itemsSketch;

  public HistogramImpl(ByteBuffer tdigestBuff, ByteBuffer itemsSketchBuff, SqlTypeName typeName) {
    if (tdigestBuff != null) {
      this.tDigest = com.tdunning.math.stats.MergingDigest.fromBytes(tdigestBuff);
    } else {
      this.tDigest = null;
    }
    if (itemsSketchBuff != null && typeName != null) {
      this.itemsSketch =
          ItemsSketch.getInstance(
              Memory.wrap(itemsSketchBuff), ItemsSketchFunctions.getSerdeFromSqlTypeName(typeName));
    } else {
      this.itemsSketch = null;
    }
  }

  @Override
  public boolean isTDigestSet() {
    return tDigest != null;
  }

  @Override
  public boolean isItemsSketchSet() {
    return itemsSketch != null;
  }

  @Override
  public double quantile(double q) {
    return tDigest.quantile(q);
  }

  /**
   * Threshold 't' determines set of values that may occur more than N/t times, where N is total
   * number of rows. If the threshold is lower than getMaximumError(), then getMaximumError() will
   * be used instead.
   *
   * <p>ErrorType = NO_FALSE_POSITIVES, this will include an item in the result list if
   * getLowerBound(item) > threshold. There will be no false positives, i.e., no Type I error. There
   * may be items omitted from the set with true frequencies greater than the threshold (false
   * negatives).
   */
  @Override
  public Set<Object> getFrequentItems(long threshold) {
    return Arrays.stream(itemsSketch.getFrequentItems(threshold, ErrorType.NO_FALSE_POSITIVES))
        .map(e -> e.getItem())
        .collect(Collectors.toSet());
  }

  @Override
  public long estimateCount(Object e) {
    return itemsSketch.getEstimate(e);
  }

  /**
   * Estimate the selectivity of a filter which may contain several range predicates and in the
   * general case is of type: col op value1 AND col op value2 AND col op value3 ...
   *
   * <p>e.g a > 10 AND a < 50 AND a >= 20 AND a <= 70 ... NOTE: 5 > a etc is ignored as this assume
   * filters have been canonicalized by predicate push down logic Even though in most cases it will
   * have either 1 or 2 range conditions, we still have to handle the general case Note - This
   * function returns the selectivity of the max range for example from >=lowerBound and <=
   * upperBound so that we do not have an underestimate
   */
  @Override
  public Double estimatedRangeSelectivity(final RexNode columnFilter) {
    if (!isTDigestSet()) {
      return null;
    }
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
      // tDigest.cdf(x) returns cdf for >=x so for range [lowerBound, UpperBound] we need to
      // subtract a delta
      // from the lowerBound [LowerBound, UpperBound] => [Upperbound, inf) - [LowerBound-delta,inf)
      // delta is the max value divided by 1000, basically 0.1% of the max value
      double delta = (highValue != null) ? highValue / 1000 : tDigest.getMax() / 1000;
      if (highValue != null && lowValue != null) {
        return scaleFactor * (tDigest.cdf(highValue) - tDigest.cdf(lowValue - delta));
      } else if (highValue != null) {
        return scaleFactor * (tDigest.cdf(highValue));
      } else {
        return scaleFactor * (1 - tDigest.cdf(lowValue - delta));
      }
    }

    return null;
  }

  @Override
  public Long estimatedPointSelectivity(final RexNode filter) {
    if (!isItemsSketchSet()) {
      return null;
    }
    if (!(filter instanceof RexCall)) {
      return null;
    }
    Object val = getLiteralValueForItemsSketch((RexCall) filter);
    if (val == null) {
      return null;
    }
    return itemsSketch.getEstimate(val);
  }

  private Range<Double> getValuesRange(List<RexNode> filterList, List<RexNode> unkownFilterList) {
    Range<Double> currentRange = Range.all();
    for (RexNode filter : filterList) {
      if (filter instanceof RexCall) {
        Double value = getLiteralValueForTDigest((RexCall) filter);
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

  private Double getLiteralValueForTDigest(final RexCall filter) {
    Double value = null;
    List<RexNode> operands = filter.getOperands();
    if (operands.size() == 2) {
      RexLiteral l = null;
      if (operands.get(1) instanceof RexLiteral) {
        l = ((RexLiteral) operands.get(1));
      } else if (operands.get(0) instanceof RexLiteral) {
        logger.warn(
            String.format(
                "Filter %s ignored as it has not yet been canonicalized by predicate push down logic, which will rewrite it to the first parameter of the operand ( 5 > a ) rewritten to ( a < 5)",
                filter.toString()));
        return null;
      }
      if (l == null) {
        return null;
      }
      switch (operands.get(0).getType().getSqlTypeName()) {
        case DATE:
          return (double) toDateValue(l);
        case TIMESTAMP:
          return (double) toTimeStampValue(l);
        case TIME:
          return (double) toTimeValue(l);
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

  private Object getLiteralValueForItemsSketch(final RexCall filter) {
    Object value = null;
    List<RexNode> operands = filter.getOperands();
    if (operands.size() == 2) {
      RexLiteral l = null;
      if (operands.get(1) instanceof RexLiteral) {
        l = ((RexLiteral) operands.get(1));
      } else if (operands.get(0) instanceof RexLiteral) {
        logger.warn(
            String.format(
                "Filter %s ignored as it has not yet been canonicalized by predicate push down logic, which will rewrite it to the first parameter of the operand ( 5 > a ) rewritten to ( a < 5)",
                filter.toString()));
        return null;
      }
      if (l == null) {
        return null;
      }
      switch (operands.get(0).getType().getSqlTypeName()) {
        case BOOLEAN:
          return (Boolean) l.getValueAs(Boolean.class);
        case DOUBLE:
        case DECIMAL:
          return (Double) l.getValueAs(Double.class);
        case VARCHAR:
          return (String) l.getValueAs(String.class);
        case FLOAT:
          return l.getValueAs(Float.class);
        case INTEGER:
          return l.getValueAs(Integer.class);
        case SMALLINT:
          return l.getValueAs(Short.class);
        case TINYINT:
          return l.getValueAs(Short.class);
          //      case VARBINARY:
        case TIME:
          return toTimeValue(l);
          //      case INTERVAL_DAY:
        case BIGINT:
          return ((BigDecimal) l.getValueAs(BigDecimal.class))
              .setScale(0, RoundingMode.HALF_UP)
              .longValue();
        case DATE:
          return toDateValue(l);
        case TIMESTAMP:
          return toTimeStampValue(l);
        default:
          return null;
      }
    }

    return null;
  }

  private static long toDateValue(final RexLiteral literal) {
    switch (literal.getType().getSqlTypeName()) {
      case DATE:
      case TIMESTAMP:
        return ((GregorianCalendar) literal.getValue()).getTimeInMillis();
      case CHAR:
      case VARCHAR:
        // Date literal could be given in 'YYYY-MM-DD' format as string
        return ValueExpressions.getDate(((NlsString) literal.getValue()).getValue());
      default:
        throw new UnsupportedOperationException(
            "Comparision between literal of type "
                + literal.getType().getSqlTypeName()
                + " and date column not supported");
    }
  }

  private static long toTimeStampValue(final RexLiteral literal) {
    switch (literal.getType().getSqlTypeName()) {
      case DATE:
      case TIMESTAMP:
        return ((GregorianCalendar) literal.getValue()).getTimeInMillis();
      case CHAR:
      case VARCHAR:
        // Timestamp literal could be given in 'YYYY-MM-DD hh:mm:ss' format as string
        return ValueExpressions.getTimeStamp(((NlsString) literal.getValue()).getValue());
      default:
        throw new UnsupportedOperationException(
            "Comparision between literal of type "
                + literal.getType().getSqlTypeName()
                + " and time column not supported");
    }
  }

  private static long toTimeValue(final RexLiteral literal) {
    switch (literal.getType().getSqlTypeName()) {
      case TIME:
        return ((GregorianCalendar) literal.getValue()).getTimeInMillis();
      case CHAR:
      case VARCHAR:
        // Time literal could be given in 'hh:mm:ss' format as string
        return ValueExpressions.getTime(((NlsString) literal.getValue()).getValue());
      default:
        throw new UnsupportedOperationException(
            "Comparision between literal of type "
                + literal.getType().getSqlTypeName()
                + " and timestamp column not supported");
    }
  }
}

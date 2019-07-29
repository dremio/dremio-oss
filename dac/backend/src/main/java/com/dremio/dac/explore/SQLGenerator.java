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

import static com.dremio.common.perf.Timer.time;
import static com.dremio.common.utils.SqlUtils.isKeyword;
import static com.dremio.common.utils.SqlUtils.quoteIdentifier;
import static com.dremio.common.utils.SqlUtils.stringLiteral;
import static com.dremio.dac.proto.model.dataset.ActionForNonMatchingValue.REPLACE_WITH_NULL;
import static com.dremio.dac.proto.model.dataset.DataType.DATE;
import static com.dremio.dac.proto.model.dataset.DataType.DATETIME;
import static com.dremio.dac.proto.model.dataset.DataType.TEXT;
import static com.dremio.dac.proto.model.dataset.DataType.TIME;
import static com.dremio.dac.proto.model.dataset.ExpressionType.ColumnReference;
import static com.dremio.dac.proto.model.dataset.ReplaceType.NULL;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.ExpressionBase.ExpressionVisitor;
import com.dremio.dac.explore.model.FieldTransformationBase;
import com.dremio.dac.explore.model.FilterBase;
import com.dremio.dac.explore.model.FromBase;
import com.dremio.dac.explore.model.FromBase.FromVisitor;
import com.dremio.dac.explore.udfs.FormatList;
import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.ConvertCase;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.ExpCalculatedField;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.ExpConvertCase;
import com.dremio.dac.proto.model.dataset.ExpConvertType;
import com.dremio.dac.proto.model.dataset.ExpExtract;
import com.dremio.dac.proto.model.dataset.ExpFieldTransformation;
import com.dremio.dac.proto.model.dataset.ExpMeasure;
import com.dremio.dac.proto.model.dataset.ExpTrim;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.ExtractMapRule;
import com.dremio.dac.proto.model.dataset.ExtractRule;
import com.dremio.dac.proto.model.dataset.FieldConvertCase;
import com.dremio.dac.proto.model.dataset.FieldConvertDateToNumber;
import com.dremio.dac.proto.model.dataset.FieldConvertDateToText;
import com.dremio.dac.proto.model.dataset.FieldConvertFloatToDecimal;
import com.dremio.dac.proto.model.dataset.FieldConvertFloatToInteger;
import com.dremio.dac.proto.model.dataset.FieldConvertFromJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertListToText;
import com.dremio.dac.proto.model.dataset.FieldConvertNumberToDate;
import com.dremio.dac.proto.model.dataset.FieldConvertTextToDate;
import com.dremio.dac.proto.model.dataset.FieldConvertToJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertToTypeIfPossible;
import com.dremio.dac.proto.model.dataset.FieldConvertToTypeWithPatternIfPossible;
import com.dremio.dac.proto.model.dataset.FieldExtract;
import com.dremio.dac.proto.model.dataset.FieldExtractList;
import com.dremio.dac.proto.model.dataset.FieldExtractMap;
import com.dremio.dac.proto.model.dataset.FieldReplaceCustom;
import com.dremio.dac.proto.model.dataset.FieldReplacePattern;
import com.dremio.dac.proto.model.dataset.FieldReplaceRange;
import com.dremio.dac.proto.model.dataset.FieldReplaceValue;
import com.dremio.dac.proto.model.dataset.FieldSimpleConvertToType;
import com.dremio.dac.proto.model.dataset.FieldSplit;
import com.dremio.dac.proto.model.dataset.FieldTrim;
import com.dremio.dac.proto.model.dataset.FieldUnnestList;
import com.dremio.dac.proto.model.dataset.Filter;
import com.dremio.dac.proto.model.dataset.FilterByType;
import com.dremio.dac.proto.model.dataset.FilterCleanData;
import com.dremio.dac.proto.model.dataset.FilterConvertibleData;
import com.dremio.dac.proto.model.dataset.FilterConvertibleDataWithPattern;
import com.dremio.dac.proto.model.dataset.FilterCustom;
import com.dremio.dac.proto.model.dataset.FilterPattern;
import com.dremio.dac.proto.model.dataset.FilterRange;
import com.dremio.dac.proto.model.dataset.FilterValue;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.FromSubQuery;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.Join;
import com.dremio.dac.proto.model.dataset.JoinCondition;
import com.dremio.dac.proto.model.dataset.MeasureType;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.proto.model.dataset.ReplaceType;
import com.dremio.dac.proto.model.dataset.SplitRule;
import com.dremio.dac.proto.model.dataset.TrimType;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.service.errors.ClientErrorException;
import com.google.common.base.Joiner;


/**
 * Generate the SQL query from a dataset state
 */
class SQLGenerator {
  private static final Logger logger = LoggerFactory.getLogger(SQLGenerator.class);

  private final ExtractRecommender extractRecommender = new ExtractRecommender();
  private final ExtractListRecommender extractListRecommender = new ExtractListRecommender();
  private final ExtractMapRecommender extractMapRecommender = new ExtractMapRecommender();
  private final ReplaceRecommender replaceRecommender = new ReplaceRecommender();
  private final SplitRecommender splitRecommender = new SplitRecommender();

  /** max varchar length when converting to varchar */
  private static final long MAX_VARCHAR = 2048;

  public static String generateSQL(VirtualDatasetState vss){
   return new SQLGenerator().innerGenerateSQL(vss);
  }

  public SQLGenerator(){}

  public static String getTableAlias(From from) {
    return new FromVisitor<String>() {
      @Override
      public String visit(FromTable table) throws Exception {
        if (table.getAlias() != null) {
          return table.getAlias();
        } else {
          return lastElement(table.getDatasetPath());
        }
      }

      private String lastElement(String datasetPath) {
        // TODO: check if we want to alias
        return new DatasetPath(datasetPath).getLeaf().getName();
      }

      @Override
      public String visit(FromSQL sql) throws Exception {
        if (sql.getAlias() != null) {
          return sql.getAlias();
        } else {
          return "nested_sql"; // TODO: check for alias unicity
        }
      }

      @Override
      public String visit(FromSubQuery subQuery) throws Exception {
        return subQuery.getAlias();
      }
    }.visit(from);
  }

  private static String function(String function, String params) {
    return format(
        "%s(%s)",
        function,
        params
        );
  }

  private String function(String tableName, String function, Expression... exp) {
    List<String> operands = new ArrayList<>();
    for (Expression expression : exp) {
      operands.add(eval(tableName, expression));
    }
    return function(
        function,
        Joiner.on(", ").join(operands)
        );
  }

  private String convertCase(String tableName, ConvertCase convertCase, Expression operand) {
    return function(tableName, convertCaseFunction(convertCase), operand);
  }

  private String eval(String tableName, Expression value) {
    return new EvaluatingExpressionVisitor(tableName).visit(value);
  }

  private String convertCaseFunction(ConvertCase convertCase) {
    switch (convertCase) {
    case LOWER_CASE:
      return "LOWER";
    case UPPER_CASE:
      return "UPPER";
    case TITLE_CASE:
      return "TITLE";
    default:
      throw new UnsupportedOperationException("unknown case: " + convertCase);
    }
  }

  private String trim(String tableName, TrimType trimType, Expression operand) {
    String e = eval(tableName, operand);
    return function("trim", trimTypeStr(trimType) + " ' ' from " + e);
  }

  private String trimTypeStr(TrimType trimType) {
    switch(trimType) {
    case BOTH:
      return "both";
    case LEFT:
      return "leading";
    case RIGHT:
      return "trailing";
    default:
      throw new UnsupportedOperationException("unknown trim type: " + trimType);
    }
  }

  private String convertType(String tableName, ExpConvertType convertType) {
    String e = eval(tableName, convertType.getOperand());

    return String.format(
        "clean_data_to_%s(%s, %d, %d, %s)",
        convertType.getDesiredType(),
        e,
        convertType.getCastWhenPossible() ? 1 : 0,
        convertType.getActionForNonMatchingValue() == REPLACE_WITH_NULL ? 1 : 0,
        quoted(convertType.getDefaultValue() == null || convertType.getDefaultValue().isEmpty() ?
            "0" : convertType.getDefaultValue(), convertType.getDesiredType()));
  }

  private String quoted(String value, DataType type) {
    if (type == null || type == TEXT) {
      return stringLiteral(value);
    } else if (type == DATE) {
      if (value == null) {
        return "CAST (null as DATE)";
      }

      return "DATE " + stringLiteral(value);
    } else if (type == TIME) {
      if (value == null) {
        return "CAST (null as TIME)";
      }

      return "TIME " + stringLiteral(value);
    } else if (type == DATETIME) {
      if (value == null) {
        return "CAST (null as TIMESTAMP)";
      }

      return "TIMESTAMP " + stringLiteral(value);
    } else {
      return value;
    }
  }

  private String resolveReplacementValue(ReplaceType replaceType, String replacementValue, DataType replacementColType) {
    // TODO: it would be better to remove the replacement type. A null value for replacement value should be suffice, but few
    // APIs send replaceType for NULL replacement value.
    if (replaceType == NULL || replacementValue == null) {
      return "NULL";
    }
    // make sure a replacement value is sent.
    if (replacementValue.isEmpty() && replacementColType != TEXT) {
      // we allow empty values only for text data types
      throw UserException.validationError()
          .message("valid replacement value is required")
          .build(logger);
    }

    return quoted(replacementValue, replacementColType);
  }

  private String extract(final String tableName, final ExtractRule rule, Expression operand) {
    final String value = eval(tableName, operand);
    return extractRecommender.wrapRule(rule).getFunctionExpr(value);
  }

  private String innerGenerateSQL(VirtualDatasetState state) {
    try (TimedBlock b = time("genSQL")) {
      String tableName = getTableAlias(state.getFrom());
      final List<String> evaledCols = new ArrayList<>();
      final boolean isStar;
      if (state.getColumnsList() == null || state.getColumnsList().isEmpty()) {
        evaledCols.add("*");
        isStar = true;
      } else {
        for (Column column : state.getColumnsList()) {
          String evald = eval(tableName, column.getValue());
          String formattedCol = quoteIdentifier(column.getName());
          String formatted;
          if (formattedCol.equals(evald)) {
            formatted = formattedCol;
          } else {
            formatted = String.format("%s AS %s", evald, formattedCol);
          }
          evaledCols.add(formatted);
        }
        isStar = false;
      }
      final List<String> orders = new ArrayList<>();
      if (state.getOrdersList() != null) {
        for (Order order : state.getOrdersList()) {
          orders.add(quoteIdentifier(order.getName()) + " " + order.getDirection().name());
        }
      }

      final List<String> evaledFilters = new ArrayList<>();
      if (state.getFiltersList() != null) {
        for (Filter filter : state.getFiltersList()) {
          String evald = evalFilter(tableName, filter);
          evaledFilters.add(evald);
        }
      }

      final List<String> groupBys = new ArrayList<>();
      if (state.getGroupBysList() != null) {
        for (Column groupBy : state.getGroupBysList()) {
          String evald = eval(tableName, groupBy.getValue());
          groupBys.add(evald);
        }
      }

      final List<String> joins = new ArrayList<>();
      if (state.getJoinsList() != null) {
        for (Join join : state.getJoinsList()) {
          String evald = evalJoin(tableName, join, join.getJoinAlias());
          joins.add(evald);
        }
      }

      return FromBase.unwrap(state.getFrom()).accept(new FromVisitor<String>() {
        @Override
        public String visit(FromSQL sql) throws Exception {
          return formatSQLWithSubQuery(sql.getSql(), sql.getAlias());
        }

        @Override
        public String visit(FromTable name) throws Exception {
          DatasetPath datasetPath = new DatasetPath(name.getDatasetPath());
          List<String> path = new ArrayList<>();
          for (String component : datasetPath.toPathList()) {
            path.add(quoteIdentifier(component));
          }
          String table = Joiner.on(".").join(path) + (name.getAlias() == null ? "" : " AS " + quoteIdentifier(name.getAlias()));
          return formatSQL(evaledCols, orders, table, joins, evaledFilters, groupBys);
        }

        @Override
        public String visit(FromSubQuery subQuery) throws Exception {
          String sql = innerGenerateSQL(subQuery.getSuqQuery());
          return formatSQLWithSubQuery(sql, subQuery.getAlias());
        }

        private String formatSQLWithSubQuery(String sql, String alias) {
          if (isStar && orders.isEmpty() && evaledFilters.isEmpty() && groupBys.isEmpty() && joins.isEmpty()) {
            return sql;
          } else {
            if (alias == null) {
              throw new UnsupportedOperationException("the subquery should be assigned an alias: " + sql);
            }
            return formatSQL(evaledCols, orders, "(\n" + indent(sql) + "\n) " + quoteIdentifier(alias), joins, evaledFilters, groupBys);
          }
        }

        private String indent(String sql) {
          return "  " + Joiner.on("\n  ").join(sql.split("\n"));
        }
      });
    }
  }

  private String prepareValue(final String value, DataType type) throws Exception {
    if (value == null) {
      return "NULL";
    }
    switch (type) {
      case BOOLEAN:
      case DECIMAL:
      case FLOAT:
      case INTEGER:
        return value;
      case DATE:
      case DATETIME:
      case TEXT:
      case TIME:
        return stringLiteral(value);
      default:
        throw new UnsupportedOperationException("prepareValue can not be aplied to " + type.name());
    }
  }

  private String evalFilter(final String tableName, Filter filter) {
    final String column = eval(tableName, filter.getOperand());
    final Boolean isExclude = Boolean.TRUE.equals(filter.getExclude());
    StringBuilder sql = new StringBuilder(new FilterBase.FilterVisitor<String>() {

      @Override
      public String visit(FilterCleanData clean) throws Exception {
        return (isExclude ? "not " : "") + format(
            "is_clean_data(%s, %d, %s)",
            column,
            clean.getCastWhenPossible() ? 1 : 0,
            stringLiteral(clean.getDesiredType().name()));
      }

      @Override
      public String visit(FilterByType byType) throws Exception {
        return format(
            "dremio_type_of(%s) " + (isExclude ? "<>" : "=") + " %s",
            column,
            stringLiteral(byType.getType().name()));
      }

      @Override
      public String visit(FilterRange range) throws Exception {
        StringBuilder sb = new StringBuilder();
        if (range.getLowerBound() != null) {
          if (range.getLowerBoundInclusive() != null && range.getLowerBoundInclusive()) {
            sb.append(prepareValue(range.getLowerBound(), range.getDataType())).append(isExclude ? " >= " : " <= ").append(column);
          } else {
            sb.append(prepareValue(range.getLowerBound(), range.getDataType())).append(isExclude ? " > " : " < ").append(column);
          }
          if (range.getUpperBound() != null) {
            sb.append(isExclude ? " OR " : " AND ");
          }
        }
        if (range.getUpperBound() != null) {
          if (range.getUpperBoundInclusive() != null && range.getUpperBoundInclusive()) {
            sb.append(prepareValue(range.getUpperBound(), range.getDataType())).append(isExclude ? " <= " : " >= ")
              .append(column);
          } else {
            sb.append(prepareValue(range.getUpperBound(), range.getDataType())).append(isExclude ? " < " : " > ")
              .append(column);
          }
        }
        return sb.toString();
      }

      @Override
      public String visit(FilterCustom custom) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(custom.getExpression());
        if (isExclude) {
          sb.insert(0, " ( ").append(" ) IS FALSE ");
        }
        return sb.toString();
      }

      @Override
      public String visit(FilterValue filterValue) throws Exception {
        List<String> copyValues = new ArrayList<String>(filterValue.getValuesList());
        boolean containsNull = copyValues.contains(null);
        StringBuilder sb = new StringBuilder();
        sb.append(column);
        if (containsNull) {
          sb.append(isExclude ? " IS NOT NULL" : " IS NULL");
          copyValues.remove(null);
        }
        if (!copyValues.isEmpty()) {
          if (containsNull) {
            sb.append(isExclude ? " AND " : " OR ").append(column);
          }

          if (copyValues.size() == 1) {
            String value = copyValues.get(0);
            sb.append(isExclude ? " <> " : " = ").append(prepareValue(value, filterValue.getDataType()));
          } else {
            if (isExclude) {
              sb.append(" NOT");
            }
            sb.append(" IN (");
            List<String> values = new ArrayList<>();
            for (String value : copyValues) {
              values.add(prepareValue(value, filterValue.getDataType()));
            }
            sb.append(Joiner.on(", ").join(values)).append(")");
          }
        }
        return sb.toString();
      }

      @Override
      public String visit(FilterPattern filterPattern) throws Exception {
        StringBuilder sb = new StringBuilder();
        if (isExclude) {
          sb.append("NOT ");
        }
        sb.append(replaceRecommender.wrapRule(filterPattern.getRule()).getMatchFunctionExpr(column));
        return sb.toString();
      }

      @Override
      public String visit(FilterConvertibleData value) throws Exception {
        return (isExclude ? "NOT " : "") + format(
            "is_convertible_data(%s, %d, %s)",
            column,
            1,
            stringLiteral(value.getDesiredType().name()));
      }

      @Override
      public String visit(FilterConvertibleDataWithPattern value) throws Exception {
        switch (value.getDesiredType()) {
          case DATE:
          case DATETIME:
          case TIME:
            return (isExclude ? "NOT " : "") + format(
              "is_%s(%s, %s)",
              DataTypeUtil.getStringValueForDateType(value.getDesiredType()),
              column,
              quoted(value.getPattern(), TEXT));
          default:
            throw new UnsupportedOperationException("Unknown type " + value.getDesiredType());
        }
      }
    }.visit(filter.getFilterDef()));

    if (Boolean.TRUE.equals(filter.getKeepNull())) {
      sql.append(" OR ").append(column).append(" IS NULL ");
    }

    return sql.toString();
  }

  private String evalJoin(String table, Join join, String joinAlias) {
    StringBuilder sql = new StringBuilder("");
    switch (join.getJoinType()) {
      case Inner:
        sql.append("INNER JOIN ");
        break;
      case LeftOuter:
        sql.append("LEFT JOIN ");
        break;
      case RightOuter:
        sql.append("RIGHT JOIN ");
        break;
      case FullOuter:
        sql.append("FULL JOIN ");
        break;
      default:
        throw new IllegalArgumentException("Unknown join type " + join.getJoinType().name());
    }
    sql.append(join.getRightTable()).append(" AS ").append(quoteIdentifier(joinAlias)).append(" ON ");
    List<String> conds = new ArrayList<>();
    for (JoinCondition c : join.getJoinConditionsList()) {
      conds.add(format("%s.%s = %s.%s",
          quoteIdentifier(table),
          quoteIdentifier(c.getLeftColumn()),
          quoteIdentifier(joinAlias),
          quoteIdentifier(c.getRightColumn())
      ));
    }
    sql.append(Joiner.on(" AND ").join(conds));
    return sql.toString();
  }

  private String formatSQL(List<String> evaledCols, List<String> orders,
      String parentDSSQL, List<String> evaledJoins, List<String> evaledFilters, List<String> groupBys) {
    final String filtersJoined;
    // only add parenthesis around more than one filter condition, to keep query cleaner
    filtersJoined = evaledFilters.isEmpty() ? "" :
      "\n WHERE " + (
        evaledFilters.size() == 1 ? evaledFilters.get(0) :
          "(" + Joiner.on(") AND (").join(evaledFilters) + ")");

    return String.format("SELECT %s\nFROM %s%s%s%s%s",
        Joiner.on(", ").join(evaledCols),
        parentDSSQL,
        (evaledJoins.isEmpty() ? "" : "\n " + Joiner.on("\n ").join(evaledJoins)),
        filtersJoined,
        (groupBys.size() > 0 ? "\nGROUP BY " + Joiner.on(", ").join(groupBys) : ""),
        (orders.size() > 0 ? "\nORDER BY " + Joiner.on(", ").join(orders) : ""));
  }

  private final class EvaluatingExpressionVisitor extends ExpressionVisitor<String> {

    private String tableName;

    public EvaluatingExpressionVisitor(String tableName) {
      this.tableName = checkNotNull(tableName);
    }

    @Override
    public String visit(ExpColumnReference col) throws Exception {
      String tableAlias = col.getTable();
      if (tableAlias == null && isKeyword(col.getName())) {
        tableAlias = tableName;
      }
      if (tableAlias == null) {
        return quoteIdentifier(col.getName());
      } else {
        return format("%s.%s", quoteIdentifier(tableAlias), quoteIdentifier(col.getName()));
      }
    }

    @Override
    public String visit(ExpConvertCase changeCase) throws Exception {
      return convertCase(tableName, changeCase.getConvertCase(), changeCase.getOperand());
    }

    @Override
    public String visit(ExpExtract extract) throws Exception {
      return extract(tableName, extract.getRule(), extract.getOperand());
    }

    @Override
    public String visit(ExpTrim trim) throws Exception {
      return trim(tableName, trim.getTrimType(), trim.getOperand());
    }

    @Override
    public String visit(ExpCalculatedField calculatedField) throws Exception {
      return calculatedField.getExp();
    }

    @Override
    public String visit(final ExpFieldTransformation fieldTransformation) throws Exception {
      return new EvaluatingFieldTransformationVisitor(fieldTransformation, tableName).visit(fieldTransformation.getTransformation());
    }

    @Override
    public String visit(ExpConvertType convertType) throws Exception {
      return convertType(tableName, convertType);
    }

    @Override
    public String visit(ExpMeasure measure) throws Exception {
      String operand = null;
      if (measure.getMeasureType() != MeasureType.Count_Star) {
        if (measure.getOperand() == null) {
          throw new IllegalArgumentException("Operand should be not null for measure type " + measure.getMeasureType().name());
        }
        operand = eval(measure.getOperand());
      }
      switch(measure.getMeasureType()) {
      case Average:
        return function("AVG", operand);
      case Count:
        return function("COUNT", operand);
      case Count_Distinct:
        return function("COUNT", "DISTINCT " + operand);
      case Count_Star:
        return function("COUNT", "*");
      case Maximum:
        return function("MAX", operand);
      case Minimum:
        return function("MIN", operand);
      case Standard_Deviation:
        return function("STDDEV", operand);
      case Standard_Deviation_Population:
        return function("STDDEV_POP", operand);
      case Sum:
        return function("SUM", operand);
      default:
        throw new IllegalArgumentException("Unknown measure type " + measure.getMeasureType().name());
      }
    }

    private String eval(Expression value) {
      return SQLGenerator.this.eval(tableName, value);
    }

  }

  private final class EvaluatingFieldTransformationVisitor
    extends FieldTransformationBase.FieldTransformationVisitor<String> {
    private final ExpFieldTransformation fieldTransformation;
    private final String tableName;

    private EvaluatingFieldTransformationVisitor(ExpFieldTransformation fieldTransformation, String tableName) {
      this.fieldTransformation = checkNotNull(fieldTransformation);
      this.tableName = checkNotNull(tableName);
    }

    @Override
    public String visit(FieldConvertCase convertCase) throws Exception {
      return convertCase(tableName, convertCase.getConvertCase(), fieldTransformation.getOperand());
    }

    @Override
    public String visit(FieldTrim trim) throws Exception {
      return trim(tableName, trim.getTrimType(), fieldTransformation.getOperand());
    }

    @Override
    public String visit(FieldExtract extract) throws Exception {
      return extract(tableName, extract.getRule(), fieldTransformation.getOperand());
    }

    @Override
    public String visit(FieldConvertFloatToInteger floatToInt) throws Exception {
      return format(
              "CAST(%s(%s) as INTEGER)",
              floatToInt.getRounding().name(), eval(fieldTransformation.getOperand()));
    }

    @Override
    public String visit(FieldConvertFloatToDecimal floatToDec) throws Exception {
      return format(
          "CAST(%s as DECIMAL(28, %d))",
          eval(fieldTransformation.getOperand()), floatToDec.getRoundingDecimalPlaces());
    }

    @Override
    public String visit(FieldConvertDateToText dateToText) throws Exception {
      return format(
          "TO_CHAR(%s, %s)",
          eval(fieldTransformation.getOperand()),
          stringLiteral(dateToText.getFormat()));
    }

    @Override
    public String visit(FieldConvertNumberToDate numberToDate) throws Exception {
      String op = eval(fieldTransformation.getOperand());
      String functionCall;
      String format;
      switch (numberToDate.getFormat()) {
        case EPOCH:
          format = "%s";
          break;
        case EXCEL:
          format = "(%s - 25569) * 86400";
          break;
        case JULIAN:
          format = "(%s - 2440587.5) * 86400";
          break;
        default:
          throw new ClientErrorException("only EPOCH, EXCEL and JULIAN ar valid. Got " + numberToDate.getFormat());
      }
      DataType desiredDateType = numberToDate.getDesiredType();
      switch (desiredDateType) {
        case DATE:
        case DATETIME:
        case TIME:
          functionCall = format("TO_%s(%s)", DataTypeUtil.getStringValueForDateType(desiredDateType), format);
          break;
        default:
          throw new ClientErrorException("only DATE, TIME and DATETIME ar valid. Got " + desiredDateType);
      }
      return format(
        functionCall,
        op);
    }

    @Override
    public String visit(FieldConvertDateToNumber dateToNumber) throws Exception {
      String op = eval(fieldTransformation.getOperand());
      String functionCall;
      String format;
      String ceil;
      switch (dateToNumber.getDesiredType()) {
        case INTEGER:
          ceil = "CEIL";
          break;
        case FLOAT:
          ceil = "";
          break;
        default:
          throw new ClientErrorException("only INTEGER or FLOAT are valid. Got " + dateToNumber.getDesiredType());
      }

      switch (dateToNumber.getConvertType()) {
        case DATE:
          format = "'YYYY-MM-DD'";
          break;
        case DATETIME:
          format = "'YYYY-MM-DD HH24:MI:SS.FFF'";
          break;
        case TIME:
          format = "'YYYY-MM-DD\"T\"HH24:MI:SS.FFFTZO'";
          break;
        default:
          throw new ClientErrorException("only DATE, TIME or DATETIME are valid. Got " + dateToNumber.getConvertType());
      }

      switch (dateToNumber.getFormat()) {
        case EPOCH:
          functionCall = "UNIX_TIMESTAMP(%s, " + format + ")";
          if (dateToNumber.getDesiredType() == DataType.FLOAT) {
            functionCall = format("CAST(%s as DOUBLE)", functionCall);
          }
          break;
        case EXCEL:
          functionCall = ceil + "(UNIX_TIMESTAMP(%s, " + format + ") / 86400 + 25569)";
          if (dateToNumber.getDesiredType() == DataType.INTEGER) {
            functionCall = format("CAST(%s as INTEGER)", functionCall);
          }
          break;
        case JULIAN:
          functionCall = ceil + "(UNIX_TIMESTAMP(%s, " + format + ") / 86400 + 2440587.5)";
          if (dateToNumber.getDesiredType() == DataType.INTEGER) {
            functionCall = format("CAST(%s as INTEGER)", functionCall);
          }
          break;
        default:
          throw new ClientErrorException("only EPOCH, EXCEL and JULIAN ar valid. Got " + dateToNumber.getFormat());
      }

      return format(
              functionCall,
              op);
    }

    @Override
    public String visit(FieldConvertTextToDate textToDate) throws Exception {
      String op = eval(fieldTransformation.getOperand());
      String dateFormat = stringLiteral(textToDate.getFormat());
      DataType desiredType = textToDate.getDesiredType();
      switch (desiredType) {
        case DATE:
        case DATETIME:
        case TIME:
          return format("TO_%s(%s, %s)", DataTypeUtil.getStringValueForDateType(desiredType), op, dateFormat);
      default:
        throw new IllegalArgumentException("only DATE, TIME and DATETIME ar valid. Got " + desiredType);
      }
    }

    @Override
    public String visit(FieldConvertListToText listToText) throws Exception {
      return format(
          "%s(%s, %s)",
          FormatList.NAME,
          eval(fieldTransformation.getOperand()),
          stringLiteral(listToText.getDelimiter()));
    }

    @Override
    public String visit(FieldConvertToJSON toJson) throws Exception {
      return format(
          "CAST(convert_from(convert_to(%s, 'JSON'), 'UTF8') as VARCHAR)",
          eval(fieldTransformation.getOperand()));
    }

    @Override
    public String visit(FieldUnnestList unnest) throws Exception {
      return format(
          "flatten(%s)",
          eval(fieldTransformation.getOperand()));
    }

    @Override
    public String visit(FieldReplacePattern replacePattern) throws Exception {
      ReplacePatternRule rule = replacePattern.getRule();
      final String value = eval(fieldTransformation.getOperand());
      return replaceRecommender.wrapRule(rule)
          .getFunctionExpr(value, replacePattern.getReplaceType(), replacePattern.getReplacementValue());
    }

    @Override
    public String visit(FieldReplaceCustom replaceCustom) throws Exception {
      final String value = eval(fieldTransformation.getOperand());
      // TODO: What if the expression refers to the dropped field itself?
      StringBuilder sb = new StringBuilder("CASE");
      String quotedReplacementValue =
          replaceCustom.getReplaceType() == NULL ? "NULL" :
            quoted(replaceCustom.getReplacementValue(), replaceCustom.getReplacementType());
      sb.append("\n  WHEN ").append(replaceCustom.getBooleanExpression()).append(" THEN ").append(quotedReplacementValue);
      sb.append("\n  ELSE ").append(value);
      sb.append("\nEND");
      return sb.toString();
    }

    @Override
    public String visit(FieldReplaceValue replaceValue) throws Exception {
      final String value = eval(fieldTransformation.getOperand());
      StringBuilder sb = new StringBuilder("CASE");
      final String quotedReplacementValue = resolveReplacementValue(replaceValue.getReplaceType(),
          replaceValue.getReplacementValue(), replaceValue.getReplacementType());

      boolean atLeastOneValueSel = false;
      if (replaceValue.getReplacedValuesList() != null) {
        for (String toReplace : replaceValue.getReplacedValuesList()) {
          sb.append("\n  WHEN ").append(value);
          if (toReplace == null) {
            sb.append(" IS NULL");
          } else {
            sb.append(" = ").append(quoted(toReplace, replaceValue.getReplacementType()));
          }
          sb.append(" THEN ").append(quotedReplacementValue);
          atLeastOneValueSel = true;
        }
      }
      if (replaceValue.getReplaceNull() != null && replaceValue.getReplaceNull()) {
        // Keep this to support old browser code (may be from cache)
        sb.append("\n  WHEN ").append(value).append(" IS NULL THEN ").append(quotedReplacementValue);
        atLeastOneValueSel = true;
      }

      if (!atLeastOneValueSel) {
        throw UserException.validationError()
            .message("select at least one value to replace")
            .build(logger);
      }

      sb.append("\n  ELSE ").append(value);
      sb.append("\nEND");
      return sb.toString();
    }

    @Override
    public String visit(FieldReplaceRange replaceRange) throws Exception {
      final String value = eval(fieldTransformation.getOperand());
      final String quotedReplacementValue = resolveReplacementValue(replaceRange.getReplaceType(),
          replaceRange.getReplacementValue(), replaceRange.getReplacementType());

      StringBuilder sb = new StringBuilder();
      if (replaceRange.getLowerBound() == null && replaceRange.getUpperBound() == null) {

        if (replaceRange.getReplaceType() == NULL || replaceRange.getReplacementValue() == null) {
          // We can't project nulls without casting. Casting is an issue here as we don't have the precision available here.
          sb.append("CASE\n  WHEN 1 = 0 THEN ").append(value).append("\n  ELSE NULL\nEND");
        } else if (replaceRange.getKeepNull() != null && replaceRange.getKeepNull()) {
          // basically replace everything except the null values
          sb.append("CASE\n  WHEN ")
              .append(value)
              .append(" IS NOT NULL THEN ")
              .append(quotedReplacementValue)
              .append("\n  ELSE NULL")
              .append("\nEND");
        } else {
          // replace everything
          sb.append(quotedReplacementValue);
        }
      } else {
        // It doesn't make sense for keepNulls flag when you have a range. So don't need to handle it.
        sb.append("CASE");
        sb.append("\n  WHEN ");
        if (replaceRange.getLowerBound() != null) {
          if (replaceRange.getLowerBoundInclusive() != null && replaceRange.getLowerBoundInclusive()) {
            sb.append(quoted(replaceRange.getLowerBound(), replaceRange.getReplacementType())).append(" <= ").append(value);
          } else {
            sb.append(quoted(replaceRange.getLowerBound(), replaceRange.getReplacementType())).append(" < ").append(value);
          }
          if (replaceRange.getUpperBound() != null) {
            sb.append(" AND ");
          }
        }
        if (replaceRange.getUpperBound() != null) {
          if (replaceRange.getUpperBoundInclusive() != null && replaceRange.getUpperBoundInclusive()) {
            sb.append(quoted(replaceRange.getUpperBound(), replaceRange.getReplacementType())).append(" >= ")
              .append(value);
          } else {
            sb.append(quoted(replaceRange.getUpperBound(), replaceRange.getReplacementType())).append(" > ").append(value);
          }
        }
        sb.append(" THEN ").append(quotedReplacementValue);
        sb.append("\n  ELSE ").append(value);
        sb.append("\nEND");
      }

      return sb.toString();
    }

    @Override
    public String visit(FieldExtractMap extract) throws Exception {
      if (fieldTransformation.getOperand().getType() != ColumnReference) {
        throw new IllegalArgumentException("Can only generate extract map for column reference, got " + extract.toString());
      }
      ExpColumnReference col = fieldTransformation.getOperand().getCol();
      ExtractMapRule rule = extract.getRule();

      String tableAlias = col.getTable() ==  null ? tableName : col.getTable();
      if (tableAlias == null) {
        throw new IllegalArgumentException("Can only generate extract map for column reference in a table for " + fieldTransformation);
      }

      String inputExpr = String.format("%s.%s", quoteIdentifier(tableAlias), quoteIdentifier(col.getName()));
      return extractMapRecommender.wrapRule(rule).getFunctionExpr(inputExpr);
    }

    @Override
    public String visit(FieldExtractList extract) throws Exception {
      final String value = eval(fieldTransformation.getOperand());
      return extractListRecommender.wrapRule(extract.getRule()).getFunctionExpr(value);
    }

    @Override
    public String visit(FieldSplit split) throws Exception {
      final String value = eval(fieldTransformation.getOperand());
      SplitRule rule = split.getRule();
      Integer last;
      switch (split.getPosition()) {
      case ALL:
        last = split.getMaxFields();
        break;
      case INDEX:
        last = split.getIndex();
        break;
      default:
        last = -1;
      }

      return splitRecommender.wrapRule(rule).getFunctionExpr(value, split.getPosition(), last);
    }

    private String eval(Expression value) {
      return SQLGenerator.this.eval(tableName, value);
    }

    @Override
    public String visit(FieldSimpleConvertToType toType) throws Exception {
      switch (toType.getDataType()) {
      case BOOLEAN:
      case DECIMAL:
      case GEO:
      case INTEGER:
      case LIST:
      case MAP:
      case MIXED:
      case OTHER:
        throw new IllegalArgumentException("Can't convert without parameters to type " + toType.getDataType());
      case DATE:
        return format("CAST(%s as DATE)", eval(fieldTransformation.getOperand()));
      case DATETIME:
        return format("CAST(%s as TIMESTAMP)", eval(fieldTransformation.getOperand()));
      case TIME:
        return format("CAST(CAST(%s as TIMESTAMP) as TIME)", eval(fieldTransformation.getOperand()));
      case FLOAT:
        return format("CAST(%s as DOUBLE)", eval(fieldTransformation.getOperand()));
      case BINARY:
        return format("CONVERT_TO(%s ,'UTF8')", eval(fieldTransformation.getOperand()));
      case TEXT:
        return format("CAST(%s as VARCHAR(%d))", eval(fieldTransformation.getOperand()), MAX_VARCHAR);
      default:
        throw new UnsupportedOperationException("Unknown type " + toType.getDataType());
      }
    }

    @Override
    public String visit(FieldConvertToTypeIfPossible toTypeIfPossible) throws Exception {
      String e = eval(fieldTransformation.getOperand());
      return format(
          "CONVERT_TO_%s(%s, %d, %d, %s)",
          toTypeIfPossible.getDesiredType(),
          e,
          1,
          toTypeIfPossible.getActionForNonMatchingValue() == REPLACE_WITH_NULL ? 1 : 0,
          quoted(toTypeIfPossible.getDefaultValue() == null ? "0" : toTypeIfPossible.getDefaultValue(), toTypeIfPossible.getDesiredType()));
    }

    @Override
    public String visit(FieldConvertToTypeWithPatternIfPossible toTypeIfPossible) throws Exception {
      String e = eval(fieldTransformation.getOperand());

      DataType desiredType = toTypeIfPossible.getDesiredType();

      switch (desiredType) {
        case DATE:
        case DATETIME:
        case TIME:
          return format(
            "TO_%s(%s, %s, %d)",
            DataTypeUtil.getStringValueForDateType(desiredType),
            e,
            quoted(toTypeIfPossible.getPattern(), TEXT),
            toTypeIfPossible.getActionForNonMatchingValue() == REPLACE_WITH_NULL ? 1 : 0);
        default:
          throw new UnsupportedOperationException("Unknown type " + desiredType);
      }
    }

    @Override
    public String visit(FieldConvertFromJSON fromJson) throws Exception {
      String e = eval(fieldTransformation.getOperand());
      return format("convert_from(%s, 'JSON')", e);
    }
  }
}

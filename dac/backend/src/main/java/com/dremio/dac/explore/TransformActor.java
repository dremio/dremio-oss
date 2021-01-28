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

import static com.dremio.common.utils.Protos.listNotNull;
import static com.dremio.common.utils.ProtostuffUtil.copy;
import static com.dremio.dac.proto.model.dataset.ActionForNonMatchingValue.REPLACE_WITH_DEFAULT;
import static com.dremio.dac.proto.model.dataset.ActionForNonMatchingValue.REPLACE_WITH_NULL;
import static com.dremio.dac.proto.model.dataset.DataType.DATETIME;
import static com.dremio.dac.proto.model.dataset.DataType.TEXT;
import static com.dremio.dac.proto.model.dataset.ExpressionType.ColumnReference;
import static com.dremio.dac.proto.model.dataset.FieldTransformationType.UnnestList;
import static com.dremio.dac.proto.model.dataset.OrderDirection.ASC;
import static com.dremio.dac.proto.model.dataset.ReplaceType.VALUE;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.ExpressionBase;
import com.dremio.dac.explore.model.FieldTransformationBase;
import com.dremio.dac.explore.model.FieldTransformationBase.FieldTransformationVisitor;
import com.dremio.dac.explore.model.TransformBase;
import com.dremio.dac.model.common.ValidationErrorMessages;
import com.dremio.dac.proto.model.dataset.ActionForNonMatchingValue;
import com.dremio.dac.proto.model.dataset.Column;
import com.dremio.dac.proto.model.dataset.DataType;
import com.dremio.dac.proto.model.dataset.Dimension;
import com.dremio.dac.proto.model.dataset.ExpCalculatedField;
import com.dremio.dac.proto.model.dataset.ExpColumnReference;
import com.dremio.dac.proto.model.dataset.ExpConvertCase;
import com.dremio.dac.proto.model.dataset.ExpConvertType;
import com.dremio.dac.proto.model.dataset.ExpExtract;
import com.dremio.dac.proto.model.dataset.ExpFieldTransformation;
import com.dremio.dac.proto.model.dataset.ExpMeasure;
import com.dremio.dac.proto.model.dataset.ExpTrim;
import com.dremio.dac.proto.model.dataset.Expression;
import com.dremio.dac.proto.model.dataset.ExpressionType;
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
import com.dremio.dac.proto.model.dataset.FieldTransformation;
import com.dremio.dac.proto.model.dataset.FieldTrim;
import com.dremio.dac.proto.model.dataset.FieldUnnestList;
import com.dremio.dac.proto.model.dataset.Filter;
import com.dremio.dac.proto.model.dataset.FilterCleanData;
import com.dremio.dac.proto.model.dataset.FilterConvertibleData;
import com.dremio.dac.proto.model.dataset.FilterConvertibleDataWithPattern;
import com.dremio.dac.proto.model.dataset.FilterRange;
import com.dremio.dac.proto.model.dataset.FilterType;
import com.dremio.dac.proto.model.dataset.Join;
import com.dremio.dac.proto.model.dataset.JoinCondition;
import com.dremio.dac.proto.model.dataset.JoinType;
import com.dremio.dac.proto.model.dataset.Measure;
import com.dremio.dac.proto.model.dataset.MeasureType;
import com.dremio.dac.proto.model.dataset.Order;
import com.dremio.dac.proto.model.dataset.OrderDirection;
import com.dremio.dac.proto.model.dataset.TransformAddCalculatedField;
import com.dremio.dac.proto.model.dataset.TransformConvertCase;
import com.dremio.dac.proto.model.dataset.TransformConvertToSingleType;
import com.dremio.dac.proto.model.dataset.TransformCreateFromParent;
import com.dremio.dac.proto.model.dataset.TransformDrop;
import com.dremio.dac.proto.model.dataset.TransformExtract;
import com.dremio.dac.proto.model.dataset.TransformField;
import com.dremio.dac.proto.model.dataset.TransformFilter;
import com.dremio.dac.proto.model.dataset.TransformGroupBy;
import com.dremio.dac.proto.model.dataset.TransformJoin;
import com.dremio.dac.proto.model.dataset.TransformLookup;
import com.dremio.dac.proto.model.dataset.TransformRename;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.proto.model.dataset.TransformSorts;
import com.dremio.dac.proto.model.dataset.TransformSplitByDataType;
import com.dremio.dac.proto.model.dataset.TransformTrim;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.proto.model.dataset.VirtualDatasetState;
import com.dremio.dac.service.errors.ClientErrorException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.google.common.base.Optional;

/**
 * Abstract class that actually applies a single transformation. Uses a visitor
 * pattern to dispatch to correct TransformBase implementation.
 */
abstract class TransformActor implements TransformBase.TransformVisitor<TransformResult> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TransformActor.class);

  private final DatasetStateMutator m;
  private final boolean preview;
  private final String username;
  private final QueryExecutor executor;

  public TransformActor(
      VirtualDatasetState initialState,
      boolean preview,
      String username,
      QueryExecutor executor) {
    super();
    this.m = new DatasetStateMutator(username, copy(initialState), preview);
    this.preview = preview;
    this.username = username;
    this.executor = executor;
  }

  @Override
  public TransformResult visit(TransformLookup lookup) throws Exception {
    throw new UnsupportedOperationException("NYI");
  }

  @Override
  public TransformResult visit(TransformUpdateSQL updateSQL) throws Exception {
    final SqlQuery query = new SqlQuery(updateSQL.getSql(), updateSQL.getSqlContextList(), username, updateSQL.getEngineName());
    m.setSql(getMetadata(query));
    return m.result();
  }

  protected abstract QueryMetadata getMetadata(SqlQuery query);

  protected abstract boolean hasMetadata();

  protected abstract QueryMetadata getMetadata();

  protected abstract Optional<BatchSchema> getBatchSchema();

  protected abstract Optional<List<ParentDatasetInfo>> getParents();

  protected abstract Optional<List<FieldOrigin>> getFieldOrigins();

  protected abstract Optional<List<ParentDataset>> getGrandParents();

  @Override
  public TransformResult visit(TransformJoin join) throws Exception {
    DatasetPath rightPath = new DatasetPath(join.getRightTableFullPathList());
    final String joinAlias = "join_" + rightPath.getLeaf().getName();
    m.nest();
    m.addJoin(new Join(preview ? JoinType.FullOuter : join.getJoinType(), rightPath.toPathString(), joinAlias)
        .setJoinConditionsList(join.getJoinConditionsList()));
    m.updateColumnTables();

    List<String> columns = new ArrayList<>();
    List<String> joinedColumns = new ArrayList<>();
    List<String> allJoinedColumns = new ArrayList<>();
    columns.addAll(executor.getColumnList(username, rightPath));

    final int edge = m.columnCount();
    for (JoinCondition jc : join.getJoinConditionsList()) {
      columns.remove(columns.indexOf(jc.getRightColumn()));
      final String rightCol = m.uniqueColumnName(jc.getRightColumn());
      m.moveColumn(m.indexOfCol(jc.getLeftColumn()), edge);
      m.addColumn(edge, new Column(rightCol, new Expression(ExpressionType.ColumnReference)
          .setCol(new ExpColumnReference(jc.getRightColumn()).setTable(joinAlias))));
      allJoinedColumns.add(jc.getLeftColumn());
      allJoinedColumns.add(rightCol);
      if (preview) {
        if (join.getJoinType() == JoinType.LeftOuter || join.getJoinType() == JoinType.Inner) {
          joinedColumns.add(jc.getLeftColumn());
        }
        if (join.getJoinType() == JoinType.RightOuter || join.getJoinType() == JoinType.Inner) {
          joinedColumns.add(rightCol);
        }
      }
    }

    for (String column : columns) {
      m.addColumn(new Column(m.uniqueColumnName(column),
          new Expression(ExpressionType.ColumnReference).setCol(new ExpColumnReference(column).setTable(joinAlias))));
    }

    if (preview && join.getJoinType() != JoinType.FullOuter) {
      return m.result().added(allJoinedColumns).setRowDeletionMarkerColumns(joinedColumns);
    } else {
      return m.result().added(allJoinedColumns);
    }
  }

  @Override
  public TransformResult visit(TransformSort sort) throws Exception {
    OrderDirection order = sort.getOrder();
    if (order == null) {
      order = ASC;
      sort.setOrder(order);
    }
    String sortedColumnName = sort.getSortedColumnName();

    flattenIfNeeded(sortedColumnName);

    return new TransformSorts().setColumnsList(asList(new Order(sortedColumnName, order))).accept(this);
  }

  @Override
  public TransformResult visit(TransformSorts sortMultiple) throws Exception {
    List<Order> columnsList = sortMultiple.getColumnsList();
    boolean needsNesting = false;
    for (Order o : columnsList) {
      if (isFlattened(m.findColValue(o.getName()))) {
        needsNesting = true;
      }
    }
    if (needsNesting) { m.nest(); }
    m.setOrdersList(columnsList);
    TransformResult r = m.result();
    for (Order order : columnsList) {
      r = r.modified(order.getName());
    }
    return r;
  }

  @Override
  public TransformResult visit(TransformDrop drop) throws Exception {
    String droppedColumnName = drop.getDroppedColumnName();
    if (!preview) {
      m.dropColumn(droppedColumnName);
    }
    return m.result().removed(droppedColumnName);
  }

  @Override
  public TransformResult visit(TransformRename rename) throws Exception {
    flattenIfNeeded(rename.getOldColumnName());
    String oldCol = rename.getOldColumnName();
    String newCol = rename.getNewColumnName();
    return m.rename(oldCol, newCol);
  }

  @Override
  public TransformResult visit(TransformConvertCase convertCase) throws Exception {
    flattenIfNeeded(convertCase.getColumnName());
    String oldCol = convertCase.getColumnName();
    String newCol = convertCase.getNewColumnName();
    Expression sourceColExp = m.findColValueForModification(oldCol);
    ExpressionBase caseExp = new ExpConvertCase(convertCase.getConvertCase(), sourceColExp);
    boolean dropSourceColumn = convertCase.getDropSourceColumn();
    return m.apply(oldCol, newCol, caseExp, dropSourceColumn);
  }

  @Override
  public TransformResult visit(TransformTrim trim) throws Exception {
    flattenIfNeeded(trim.getColumnName());
    String oldCol = trim.getColumnName();
    String newCol = trim.getNewColumnName();
    Expression sourceColExp = m.findColValueForModification(oldCol);
    ExpressionBase trimExp = new ExpTrim(sourceColExp, trim.getTrimType());
    return m.apply(oldCol, newCol, trimExp, trim.getDropSourceColumn());
  }

  @Override
  public TransformResult visit(final TransformExtract extract) throws Exception {
    flattenIfNeeded(extract.getSourceColumnName());
    String oldCol = extract.getSourceColumnName();
    String newCol = extract.getNewColumnName();
    Expression p = m.findColValueForModification(oldCol);
    ExpressionBase e = new ExpExtract(extract.getRule(), p);
    return m.apply(oldCol, newCol, e, extract.getDropSourceColumn());
  }

  @Override
  public TransformResult visit(TransformAddCalculatedField addCalculatedField) {
    String oldCol = addCalculatedField.getSourceColumnName();
    String newCol = addCalculatedField.getNewColumnName();
    // nest unconditionally, we are not certain if the expression provided by the user
    // only contains references to the oldCol
    // TODO - improve if we decide to parse the expression string to find the referenced columns
    m.nest();
    Expression e = new ExpCalculatedField(addCalculatedField.getExpression()).wrap();
    return m.apply(oldCol, newCol, e, addCalculatedField.getDropSourceColumn());
  }

  @Override
  public TransformResult visit(TransformField field) throws Exception {
    final String oldCol = field.getSourceColumnName();
    if (oldCol == null) {
      throw new ClientErrorException("sourceColumnName is missing in field transformation");
    }
    boolean dropSourceColumn = field.getDropSourceColumn() == null ? true : field.getDropSourceColumn();
    final String newCol = field.getNewColumnName() == null ? oldCol : field.getNewColumnName();
    if (newCol.equalsIgnoreCase(oldCol) && !dropSourceColumn) {
      throw UserException.validationError()
        .message("You cannot use a column name that already exists in the table")
        .build(logger);
    }
    // validation and defaults go here
    class TransformFieldVisitor extends FieldTransformationVisitor<FieldTransformationBase> {
      private boolean nestApplied = false;
      public boolean hasNestApplied() { return this.nestApplied; }

      @Override
      public FieldTransformationBase visit(FieldConvertCase convertCase) throws Exception {
        return convertCase;
      }

      @Override
      public FieldTransformationBase visit(FieldTrim trim) throws Exception {
        return trim;
      }

      @Override
      public FieldTransformationBase visit(FieldExtract extract) throws Exception {
        return extract;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertFloatToInteger floatToInt) throws Exception {
        return floatToInt;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertFloatToDecimal floatToDec) throws Exception {
        throw new ClientErrorException("Decimal data type is disabled!");
      }

      @Override
      public FieldTransformationBase visit(FieldConvertDateToText dateToText) throws Exception {
        return dateToText;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertNumberToDate numberToDate) throws Exception {
        numberToDate.setDesiredType(checkDateTimeDesiredType(numberToDate.getDesiredType()));
        return numberToDate;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertDateToNumber dateToNumber) throws Exception {
        dateToNumber.setDesiredType(checkNumberDesiredType(dateToNumber.getDesiredType()));
        return dateToNumber;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertTextToDate textToDate) throws Exception {
        textToDate.setDesiredType(checkDateTimeDesiredType(textToDate.getDesiredType()));

        ActionForNonMatchingValue actionForNonMatchingValue = textToDate.getActionForNonMatchingValue();

        // we can exit early if no non-matching action was defined
        if (actionForNonMatchingValue == null) {
          return textToDate;
        }

        if (actionForNonMatchingValue == ActionForNonMatchingValue.DELETE_RECORDS) {
          m.findColValueForModification(oldCol);
          TransformActor.this.visit(new TransformFilter(oldCol, new FilterConvertibleDataWithPattern(textToDate.getDesiredType(), textToDate.getFormat()).wrap()));
        }

        FieldConvertToTypeWithPatternIfPossible convert = new FieldConvertToTypeWithPatternIfPossible(textToDate.getDesiredType(), textToDate.getFormat(), actionForNonMatchingValue);
        return convert;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertListToText listToText) throws Exception {
        return listToText;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertToJSON toJson) throws Exception {
        return toJson;
      }

      @Override
      public FieldTransformationBase visit(FieldUnnestList unnest) throws Exception {
        return unnest;
      }

      @Override
      public FieldTransformationBase visit(FieldReplacePattern replacePattern) throws Exception {
        return replacePattern;
      }

      @Override
      public FieldTransformationBase visit(FieldReplaceCustom replaceCustom) throws Exception {
        if (replaceCustom.getReplaceType() == null) {
          replaceCustom.setReplaceType(VALUE);
        }
        if (replaceCustom.getReplacementType() == null) {
          replaceCustom.setReplacementType(TEXT);
        }
        return replaceCustom;
      }

      @Override
      public FieldTransformationBase visit(FieldReplaceValue replaceValue) throws Exception {
        if (replaceValue.getReplaceType() == null) {
          replaceValue.setReplaceType(VALUE);
        }
        if (replaceValue.getReplacementType() == null) {
          replaceValue.setReplacementType(TEXT);
        }
        if (!replaceValue.getReplaceNull()
            && (replaceValue.getReplacedValuesList() == null || replaceValue.getReplacedValuesList().isEmpty())) {
          throw new ClientErrorException("no value selected for replace");
        }
        return replaceValue;
      }

      @Override
      public FieldTransformationBase visit(FieldReplaceRange replaceRange) throws Exception {
        if (replaceRange.getReplaceType() == null) {
          replaceRange.setReplaceType(VALUE);
        }
        if (replaceRange.getReplacementType() == null) {
          replaceRange.setReplacementType(TEXT);
        }
        return replaceRange;
      }

      @Override
      public FieldTransformationBase visit(FieldExtractMap extract) throws Exception {
        Expression p = m.findColValueForModification(oldCol);
        // Dremio does not support referencing a subcolumn out of a map produced by a function.
        // Example of invalid expression: convert_from(a_col,'JSON').a_nested_column
        // Instead we move the function that produces the map into a subquery and make
        // the map extraction happen in an outer query.
        if (p.getType() != ColumnReference) {
          m.nest();
          nestApplied = true;
        }
        return extract;
      }

      @Override
      public FieldTransformationBase visit(FieldExtractList extract) throws Exception {
        Expression p = m.findColValueForModification(oldCol);
        // Dremio does not support referencing an array produced by a function.
        // Example of invalid expression: convert_from(a_col,'JSON')[0]
        // Instead we move the function that produces the list into a subquery and make
        // the array extraction happen in an outer query
        if (p.getType() != ColumnReference) {
          m.nest();
          nestApplied = true;
        }
        return extract;
      }

      @Override
      public FieldTransformationBase visit(FieldSplit split) throws Exception {
        // TODO: validation
        return split;
      }

      @Override
      public FieldTransformationBase visit(FieldSimpleConvertToType toType) throws Exception {
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
        case DATETIME:
        case TIME:
        case FLOAT:
        case BINARY:
          // valid under some conditions
          // TODO: validate incoming type
        case TEXT:
          // valid
          break;
        default:
          throw new UnsupportedOperationException("Unknown type " + toType.getDataType());
        }
        return toType;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertToTypeIfPossible toTypeIfPossible) throws Exception {
        switch (toTypeIfPossible.getDesiredType()) {
        case BOOLEAN:
        case DECIMAL:
        case GEO:
        case LIST:
        case MAP:
        case MIXED:
        case OTHER:
          throw new IllegalArgumentException(
              "Can't convert without parameters to type " + toTypeIfPossible.getDesiredType());
        case DATE:
        case DATETIME:
        case TIME:
        case BINARY:
          return visit(new FieldSimpleConvertToType(toTypeIfPossible.getDesiredType()));
        case TEXT:
        case INTEGER:
        case FLOAT:
          if (toTypeIfPossible.getActionForNonMatchingValue() == ActionForNonMatchingValue.DELETE_RECORDS) {
            // result ignored, this is used to nest in the case where the oldCol requires it. Below in the call
            // to add the filter, this nesting will already have taken place. In some cases the column will not require
            // nesting, but the adding the filter will. Example, we can nest flatten within a call to another function,
            // but we cannot support it in the filter list. So this allows us to avoid the nesting in the convert
            // type, after a flatten, converting non-matching values to null, but get nesting in the case with converting
            // type and filtering out non-matching rows (when it is on a flattened column).
            m.findColValueForModification(oldCol);
            TransformActor.this.visit(new TransformFilter(oldCol, new FilterConvertibleData(toTypeIfPossible.getDesiredType()).wrap()));
          }
          // valid under some conditions
          // TODO: validate incoming type
          break;
        default:
          throw new UnsupportedOperationException("Unknown type " + toTypeIfPossible.getDesiredType());
        }
        return toTypeIfPossible;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertToTypeWithPatternIfPossible toTypeIfPossible) throws Exception {
        return toTypeIfPossible;
      }

      // todo: move this to validator
      private DataType checkDateTimeDesiredType(DataType desiredType) {
        DataType result = desiredType;
        if (desiredType != null) {
          switch (desiredType) {
          case DATE:
          case DATETIME:
          case TIME:
            break;
          default:
            ValidationErrorMessages messages = new ValidationErrorMessages();
            messages.addFieldError("desiredType", "should be one of DATE, TIME, DATETIME");
            throw new ClientErrorException("desiredType should be one of DATE, TIME, DATETIME", messages);
          }
        } else {
          result = DATETIME;
        }
        return result;
      }

      // todo: move this to validator
      private DataType checkNumberDesiredType(DataType desiredType) {
        switch (desiredType) {
        case INTEGER:
        case FLOAT:
          break;
        default:
          ValidationErrorMessages messages = new ValidationErrorMessages();
          messages.addFieldError("desiredType", "should be one of INTEGER, FLOAT");
          throw new ClientErrorException("desiredType should be one of INTEGER, FLOAT", messages);
        }
        return desiredType;
      }

      @Override
      public FieldTransformationBase visit(FieldConvertFromJSON fromJson) throws Exception {
        return fromJson;
      }
    }
    TransformFieldVisitor visitor = new TransformFieldVisitor();

    FieldTransformation fieldTransformation = visitor.visit(field.getFieldTransformation()).wrap();
    if (!visitor.hasNestApplied() && isFlattened(m.findColValue(oldCol))) {
      m.nest();
    }
    // the visitor may have changed columns and must happen before
    Expression p = m.findColValueForModification(oldCol);
    ExpressionBase e = new ExpFieldTransformation(fieldTransformation, p);
    return m.apply(oldCol, newCol, e, dropSourceColumn);
  }

  @Override
  public TransformResult visit(TransformConvertToSingleType convertToSingleType) throws Exception {
    flattenIfNeeded(convertToSingleType.getSourceColumnName());
    String oldCol = convertToSingleType.getSourceColumnName();
    String newCol = convertToSingleType.getNewColumnName();
    Expression p = m.findColValueForModification(oldCol);
    ActionForNonMatchingValue actionForNonMatchingValue = convertToSingleType.getActionForNonMatchingValue();
    switch (actionForNonMatchingValue) {
    case DELETE_RECORDS:
      m.addFilter(new Filter(p,
          new FilterCleanData(convertToSingleType.getDesiredType(), convertToSingleType.getCastWhenPossible()).wrap()));
      break;
    case REPLACE_WITH_DEFAULT:
      final String defaultValue = convertToSingleType.getDefaultValue();
      if (defaultValue == null || defaultValue.isEmpty()) {
        throw new IllegalArgumentException("default value required when " + REPLACE_WITH_DEFAULT);
      }
      break;
    case REPLACE_WITH_NULL:
      break;
    default:
      throw new UnsupportedOperationException(actionForNonMatchingValue.name());
    }
    ExpressionBase e = new ExpConvertType(
        convertToSingleType.getDesiredType(),
        convertToSingleType.getCastWhenPossible(),
        actionForNonMatchingValue,
        p)
        .setDefaultValue(convertToSingleType.getDefaultValue());
    return m.apply(oldCol, newCol, e, convertToSingleType.getDropSourceColumn());
  }

  @Override
  public TransformResult visit(TransformSplitByDataType splitByDataType) throws Exception {
    flattenIfNeeded(splitByDataType.getSourceColumnName());
    String oldCol = splitByDataType.getSourceColumnName();
    Expression p = m.findColValueForModification(oldCol);
    List<DataType> selectedTypes = splitByDataType.getSelectedTypesList();
    TransformResult transformResult = m.result();
    int index = m.indexOfCol(oldCol);
    for (DataType dataType : selectedTypes) {
      ++index;
      // TODO: validate col
      String newCol = splitByDataType.getNewColumnNamePrefix() + dataType.name();
      ExpressionBase e = new ExpConvertType(dataType, false, REPLACE_WITH_NULL, p);
      m.addColumn(index, new Column(newCol, e.wrap()));
      transformResult = transformResult.added(newCol);
    }
    if (splitByDataType.getDropSourceColumn()) {
      if (!preview) {
        m.dropColumn(oldCol);
      }
      transformResult = transformResult.removed(oldCol);
    }
    return transformResult;
  }

  @Override
  public TransformResult visit(TransformFilter filter) throws Exception {
    boolean nestRequired = false;
    // Reasons for requiring a nest:
    // #1: an unnest is present in the column to be filtered
    // #2: column to be filtered is an aggregate field (measures, such as SUM, etc.)
    // In both of these cases, request nesting into subquery before applying the filter
    if (canNotBeFiltered(m.findColValue(filter.getSourceColumnName()))) {
      nestRequired = true;
    }
    if (filter.getFilter().getType() == FilterType.Range) {
      FilterRange filterRange = filter.getFilter().getRange();
      if (filterRange.getLowerBound() == null && filterRange.getUpperBound() == null) {
        throw UserException.validationError()
          .message("At least one bound should not be null.")
          .build(logger);
      }
    } else if (filter.getFilter().getType() == FilterType.Custom) {
      // TODO - improve if we decide to parse the expression string to find the referenced columns
      nestRequired = true;
    }

    if (nestRequired) {
      m.nest();
    }

    final Expression p = m.findColValue(filter.getSourceColumnName());
    m.addFilter(
        new Filter(p, filter.getFilter())
        .setExclude(filter.getExclude())
        .setKeepNull(filter.getKeepNull())
        );
    return m.result();
  }

  @Override
  public TransformResult visit(TransformGroupBy groupBy) throws Exception {
    List<Column> newColumns = new ArrayList<>();
    List<Column> groupBys = new ArrayList<>();
    List<Dimension> dimensions = listNotNull(groupBy.getColumnsDimensionsList());
    List<Measure> measures = listNotNull(groupBy.getColumnsMeasuresList());

    // check if group by requires nesting
    nestIfRequiredByGroupBy(dimensions, measures);

    // now that nesting has happened:
    for (Dimension dimension : dimensions) {
      Expression p = m.findColValue(dimension.getColumn());
      Column c = new Column(dimension.getColumn(), p);
      newColumns.add(c);
      groupBys.add(c);
    }

    for (Measure measure : measures) {
      Expression p;
      String nameSuffix;
      if (hasColumnRef(measure)) {
        String colName = measure.getColumn();
        p = m.findColValue(colName);
        nameSuffix = "_" + colName;
      } else {
        p = null;
        nameSuffix = "";
      }
      newColumns.add(new Column(
          measure.getType().name() + nameSuffix,
          new ExpMeasure(measure.getType()).setOperand(p).wrap()
          ));
    }

    m.groupedBy(newColumns, groupBys);
    return m.result();
  }

  private void nestIfRequiredByGroupBy(List<Dimension> dimensions, List<Measure> measures) {
    if (m.isGrouped()) {
      m.nest();
      return;
    }

    // check if we need to nest the query because of dimensions
    for (Dimension dimension : dimensions) {
      if (canNotBeInAGroupBy(m.findColValue(dimension.getColumn()))) {
        m.nest();
        return;
      }
    }

    // check if we need to nest the query because of measures
    for (Measure measure : measures) {
      if (hasColumnRef(measure)) {
        m.findColValueForModification(measure.getColumn());
      }
    }
  }

  private boolean hasColumnRef(Measure measure) {
    return measure.getType() != MeasureType.Count_Star;
  }

  // Check if an expression is a flatten
  private static class IsFlattenedVisitor extends ExpressionBase.ExpressionVisitorBase<Boolean> {
    @Override
    public Boolean visit(ExpColumnReference col) throws Exception {
      return false;
    }

    @Override
    public Boolean visit(ExpCalculatedField calculatedField) throws Exception {
      return true; // since we don't know
    }

    @Override
    public Boolean visit(ExpFieldTransformation fieldTransformation) throws Exception {
      if (fieldTransformation.getTransformation().getType() == UnnestList) {
        // really just flatten requires nesting
        return true;
      } else {
        return visit(fieldTransformation.getOperand());
      }
    }

    @Override
    public Boolean visit(ExpMeasure measure) throws Exception {
      Expression operand = measure.getOperand();
      // measures can have NULL operands such as COUNT(*)
      if (operand == null) {
        return false;
      }
      return super.visit(measure);
    }
  }

  private boolean isFlattened(Expression p) { return new IsFlattenedVisitor().visit(p); }

  private void flattenIfNeeded(String columnName) {
    if (isFlattened(m.findColValue(columnName))) {
      m.nest();
    }
  }

  // Filters are not allowed to operate directly over measure columns (SUM, etc.); as well as unnested columns
  private static class FindCannotFilterVisitor extends IsFlattenedVisitor {
    @Override
    public Boolean visit(ExpMeasure measure) throws Exception {
      return true;
    }
  }

  private Boolean canNotBeFiltered(Expression p) {
    return new FindCannotFilterVisitor().visit(p);
  }

  private static class FindUnsupportedInGroupByVisitor extends ExpressionBase.ExpressionVisitorBase<Boolean> {

    @Override
    public Boolean visit(ExpColumnReference col) throws Exception {
      return false;
    }

    @Override
    public Boolean visit(ExpCalculatedField calculatedField) throws Exception {
      return true; // since we don't know
    }

    @Override
    public Boolean visit(ExpFieldTransformation fieldTransformation) throws Exception {
      if (fieldTransformation.getTransformation().getType() == UnnestList) {
        // really just flatten requires nesting
        return true;
      } else {
        return visit(fieldTransformation.getOperand());
      }
    }

    @Override
    public Boolean visit(ExpMeasure measure) throws Exception {
      return true;// group by
    }
  }

  private Boolean canNotBeInAGroupBy(Expression p) {
    return new FindUnsupportedInGroupByVisitor().visit(p);
  }

  @Override
  public TransformResult visit(TransformCreateFromParent createFromParent) {
    throw new IllegalStateException("A transform create from parent should only be internally created");
  }
}

/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.physical.visitor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.fs.Path;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.AggPrelBase;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashToMergeExchangePrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.LimitPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.exec.store.parquet.FilterCondition;
import com.dremio.exec.store.parquet.ParquetDatasetXAttrSerDe;
import com.dremio.exec.store.parquet.ParquetScanPrel;
import com.dremio.exec.util.GlobalDictionaryBuilder;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.DictionaryEncodedColumns;
import com.dremio.service.namespace.file.proto.ParquetDatasetXAttr;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * Replace column type with integer ids
 */
public class GlobalDictionaryVisitor extends BasePrelVisitor<PrelWithDictionaryInfo, Void, RuntimeException> {

  private final RelDataType dictionaryDataType;


  public GlobalDictionaryVisitor(RelOptCluster cluster) {
    dictionaryDataType = cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
  }

  public static Prel useGlobalDictionaries(Prel prel) {
    final PrelWithDictionaryInfo p =  prel.accept(new GlobalDictionaryVisitor(prel.getCluster()), null);
    return p.getPrel();
  }

  @Override
  public PrelWithDictionaryInfo visitLeaf(LeafPrel prel, Void value) throws RuntimeException {
    if (!(prel instanceof ParquetScanPrel)) {
      return new PrelWithDictionaryInfo(prel);
    }
    return visitParquetScanPrel((ParquetScanPrel)prel, value);
  }

  // Decode HashTo*, pass through others
  @Override
  public PrelWithDictionaryInfo visitExchange(ExchangePrel exchangePrel, Void value) throws RuntimeException {
    assert exchangePrel.getInputs().size() == 1;
    PrelWithDictionaryInfo newInput = ((Prel)exchangePrel.getInput()).accept(this, value);

    if (exchangePrel.getInput() == newInput.getPrel()) {
      return new PrelWithDictionaryInfo(exchangePrel); // none of fields are encoded
    }

    if (exchangePrel instanceof HashToMergeExchangePrel || exchangePrel instanceof HashToRandomExchangePrel) {
      final List<DistributionField> distributionFields;
      if (exchangePrel instanceof HashToMergeExchangePrel) {
        distributionFields = ((HashToMergeExchangePrel) exchangePrel).getDistFields();
      } else {
        distributionFields = ((HashToRandomExchangePrel) exchangePrel).getFields();
      }
      // decode used inputs by this filter
      newInput = newInput.decodeFields(Lists.transform(distributionFields, new Function<DistributionField, Integer>() {
        @Override
        public Integer apply(DistributionField input) {
          return input.getFieldId();
        }
      }));
    }
    // pass thr rest of exchanges, exchange uses child input's row data type.
    return new PrelWithDictionaryInfo(
      (Prel)exchangePrel.copy(exchangePrel.getTraitSet(), Collections.<RelNode>singletonList(newInput.getPrel())),
      newInput.getFields());
  }

  @Override
  public PrelWithDictionaryInfo visitJoin(JoinPrel joinPrel, Void value) throws RuntimeException {
    assert joinPrel.getInputs().size() == 2;
    // visit left
    PrelWithDictionaryInfo leftInput = ((Prel)joinPrel.getLeft()).accept(this, value);
    // visit right
    PrelWithDictionaryInfo rightInput = ((Prel)joinPrel.getRight()).accept(this, value);
    if ((joinPrel.getLeft() == leftInput.getPrel()) && (joinPrel.getRight() == rightInput.getPrel())) {
      return new PrelWithDictionaryInfo(joinPrel);
    }

    final Set<Integer> fieldsUsed = Sets.newHashSet();
    final Set<Integer> leftFieldsUsed = Sets.newHashSet();
    final Set<Integer> rightFieldsUsed = Sets.newHashSet();

    final InputReferenceRexVisitor visitor = new InputReferenceRexVisitor(fieldsUsed);
    final int leftFieldCount = leftInput.getFields().length;
    final int rightFieldCount = rightInput.getFields().length;
    final int systemFieldCount = joinPrel.getSystemFieldList().size();
    final GlobalDictionaryFieldInfo[] reorderedFields = new GlobalDictionaryFieldInfo[systemFieldCount + leftFieldCount + rightFieldCount];

    // system fields are prefixed first
    for (int i = 0; i < systemFieldCount; ++i) {
      reorderedFields[i] = null;
    }

    joinPrel.getCondition().accept(visitor);

    for (int fieldIndex : fieldsUsed) {
      if (fieldIndex < leftFieldCount) {
        leftFieldsUsed.add(fieldIndex - systemFieldCount);
      } else {
        rightFieldsUsed.add(fieldIndex - (systemFieldCount + leftFieldCount));
      }
    }

    leftInput = leftInput.decodeFields(leftFieldsUsed);
    rightInput = rightInput.decodeFields(rightFieldsUsed);

    for (int i = 0; i < leftFieldCount; ++i) {
      reorderedFields[systemFieldCount + i] = leftInput.getGlobalDictionaryFieldInfo(i);
    }

    for (int i = 0; i < rightFieldCount; ++i) {
      reorderedFields[leftFieldCount + i] = rightInput.getGlobalDictionaryFieldInfo(i);
    }
    return new PrelWithDictionaryInfo((Prel)joinPrel.copy(joinPrel.getTraitSet(),
      Lists.<RelNode>newArrayList(leftInput.getPrel(), rightInput.getPrel())), reorderedFields);
  }

  @Override
  public PrelWithDictionaryInfo visitProject(ProjectPrel projectPrel, Void value) throws RuntimeException {
    assert projectPrel.getInputs().size() == 1;

    PrelWithDictionaryInfo newInput = ((Prel)projectPrel.getInput()).accept(this, value);

    if (projectPrel.getInput() == newInput.getPrel()) {
      return new PrelWithDictionaryInfo(projectPrel);
    }

    final Set<Integer> fieldsUsed = Sets.newHashSet();
    final InputReferenceRexVisitor visitor = new InputReferenceRexVisitor(fieldsUsed);
    final Map<Integer, Integer> fieldsToInputMap = Maps.newHashMap();

    for (int i = 0; i < projectPrel.getProjects().size(); ++i) {
      final RexNode expr = projectPrel.getProjects().get(i);
      if (!(expr instanceof RexInputRef)) {
        expr.accept(visitor);
      } else {
        // these fields copied from input without any modification (to be decoded further up in tree)
        fieldsToInputMap.put(i, ((RexInputRef)expr).getIndex());
      }
    }

    newInput = newInput.decodeFields(fieldsUsed);

    // create a new row data type for project
    final GlobalDictionaryFieldInfo[] reorderedFields = new GlobalDictionaryFieldInfo[projectPrel.getRowType().getFieldCount()];
    final List<RelDataTypeField> newFields = Lists.newArrayList();
    final Set<Integer> dictionaryEncodedPassThroughFields = Sets.newHashSet();

    for (RelDataTypeField field : projectPrel.getRowType().getFieldList()) {
      // set data type to int for columns which are still dictionary encoded , other wise keep the original type.
      if (fieldsToInputMap.containsKey(field.getIndex())) {
        final int inputIndex = fieldsToInputMap.get(field.getIndex());
        if (!fieldsUsed.contains(inputIndex) && newInput.hasGlobalDictionary(inputIndex)) {
          newFields.add(new RelDataTypeFieldImpl(field.getName(), field.getIndex(), dictionaryDataType));
          // Project may change field name, copy field names into reordered fields
          reorderedFields[field.getIndex()] = newInput.getGlobalDictionaryFieldInfo(inputIndex).withName(field.getName());
          dictionaryEncodedPassThroughFields.add(inputIndex);
        } else {
          newFields.add(field);
          reorderedFields[field.getIndex()] = null;
        }
      } else {
        newFields.add(field);
        reorderedFields[field.getIndex()] = null;
      }
    }
    final RelDataType rowDataType = PrelWithDictionaryInfo.toRowDataType(newFields, projectPrel.getCluster().getTypeFactory());
    final List<RexNode> newExprs = Lists.newArrayList();
    int exprIndex = 0;
    for (RexNode expr : projectPrel.getProjects()) {
      if (expr instanceof RexInputRef) {
        newExprs.add(new RexInputRef(((RexInputRef) expr).getIndex(), rowDataType.getFieldList().get(exprIndex).getType()));
      } else {
        newExprs.add(expr);
      }
      ++exprIndex;
    }

    return new PrelWithDictionaryInfo(
      (Prel)projectPrel.copy(projectPrel.getTraitSet(), newInput.getPrel(), newExprs, rowDataType), reorderedFields);
  }

  @Override
  public PrelWithDictionaryInfo visitPrel(Prel prel, Void value) throws RuntimeException {
    if (prel instanceof AggPrelBase) {
      return visitAggregation((AggPrelBase) prel, value);
    }

    if (prel instanceof FilterPrel) {
      return visitFilter((FilterPrel)prel, value);
    }

    if (prel instanceof LimitPrel) {
      return visitLimit((LimitPrel)prel, value);
    }

    final List<RelNode> inputs = Lists.newArrayList();

    boolean changed = false;
    for (Prel input : prel) {
      final PrelWithDictionaryInfo newInput = input.accept(this, value);
      if (input != newInput.getPrel()) {
        changed = true;
      }
      // If any of input fields are dictionary encoded then insert a dictionary lookup operator for this input
      if (newInput.hasDictionaryEncodedFields()) {
        inputs.add(newInput.decodeAllFields());
      } else {
        inputs.add(newInput.getPrel());
      }
    }
    if (!changed) {
      return new PrelWithDictionaryInfo(prel);
    }
    // from this point onwards none of inputs will have global dictionary encoded fields
    return new PrelWithDictionaryInfo((Prel)prel.copy(prel.getTraitSet(), inputs));
  }

  private boolean needsValue(SqlKind sqlKind) {
    return SqlKind.COUNT != sqlKind;
  }

  private PrelWithDictionaryInfo visitAggregation(AggPrelBase aggPrel, Void value) throws RuntimeException {
    assert aggPrel.getInputs().size() == 1;
    PrelWithDictionaryInfo newInput = ((Prel)aggPrel.getInput()).accept(this, value);

    if (aggPrel.getInput() == newInput.getPrel()) {
      return new PrelWithDictionaryInfo(aggPrel);
    }

    // Get list of input fields which are used
    final Set<Integer> fieldsUsed = Sets.newHashSet();
    for (AggregateCall call : aggPrel.getAggCallList()) {
      if (needsValue(call.getAggregation().getKind())) {
        fieldsUsed.addAll(call.getArgList());
      }
    }

    // decode few fields from input, pass through other fields without decoding.
    newInput = newInput.decodeFields(fieldsUsed);

    final GlobalDictionaryFieldInfo[] reorderedFields = new GlobalDictionaryFieldInfo[aggPrel.getRowType().getFieldCount()];
    // reorder fields based on groupsets
    Iterator<Integer> groupByFields = aggPrel.getGroupSet().iterator();
    int i = 0;
    while (groupByFields.hasNext()) {
      // group by field may have been decoded in earlier call, so carry over that information.
      reorderedFields[i++] = newInput.getGlobalDictionaryFieldInfo(groupByFields.next());
    }
    for (; i< reorderedFields.length; i++) {
      reorderedFields[i] = null; // these are aggregated fields we don't need to decode
    }
    return new PrelWithDictionaryInfo(
      (Prel)aggPrel.copy(aggPrel.getTraitSet(),
        newInput.getPrel(),
        aggPrel.indicator,
        aggPrel.getGroupSet(),
        aggPrel.getGroupSets(),
        aggPrel.getAggCallList()),
      reorderedFields);
  }

  private PrelWithDictionaryInfo visitFilter(FilterPrel filterPrel, Void value) {
    assert filterPrel.getInputs().size() == 1;

    PrelWithDictionaryInfo newInput = ((Prel)filterPrel.getInput()).accept(this, value);

    if (filterPrel.getInput() == newInput.getPrel()) {
      return new PrelWithDictionaryInfo(filterPrel); // none of fields are encoded
    }

    final Set<Integer> fieldsUsed = Sets.newHashSet();
    final InputReferenceRexVisitor visitor = new InputReferenceRexVisitor(fieldsUsed);
    filterPrel.getCondition().accept(visitor);

    // decode used inputs by this filter
    newInput = newInput.decodeFields(fieldsUsed);

    return new PrelWithDictionaryInfo((Prel)filterPrel.copy(filterPrel.getTraitSet(), newInput.getPrel(), filterPrel.getCondition()),
      newInput.getFields());
  }

  // Pass through do not decode.
  private PrelWithDictionaryInfo visitLimit(LimitPrel limitPrel, Void value) {
    final PrelWithDictionaryInfo newInput = ((Prel)limitPrel.getInput()).accept(this, value);
    if (limitPrel.getInput() == newInput.getPrel()) {
      return new PrelWithDictionaryInfo(limitPrel);
    }
    return new PrelWithDictionaryInfo((Prel)limitPrel.copy(limitPrel.getTraitSet(), Collections.<RelNode>singletonList(newInput.getPrel())), newInput.getFields());
  }

  private RelDataTypeField dictionaryEncodedField(RelDataTypeField field) {
    return new RelDataTypeFieldImpl(field.getName(), field.getIndex(), dictionaryDataType);
  }

  private PrelWithDictionaryInfo visitParquetScanPrel(ParquetScanPrel parquetScanPrel, Void value) throws RuntimeException {
    final ReadDefinition readDefinition = parquetScanPrel.getTableMetadata().getReadDefinition();

    if (readDefinition == null || readDefinition.getExtendedProperty() == null) {
      return new PrelWithDictionaryInfo(parquetScanPrel);
    }

    // Make sure we don't apply global dictionary on columns that have conditions pushed into the scan
    final Set<String> columnsPushedToScan = new HashSet<>();
    if (parquetScanPrel.getConditions() != null) {
      Iterables.addAll(columnsPushedToScan, Iterables.transform(parquetScanPrel.getConditions(), FilterCondition.EXTRACT_COLUMN_NAME));
    }

    final Map<String, String> dictionaryEncodedColumnsToDictionaryFilePath = Maps.newHashMap();
    long dictionaryVersion = -1;

    final ParquetDatasetXAttr xAttr = ParquetDatasetXAttrSerDe.PARQUET_DATASET_XATTR_SERIALIZER.revert(
      readDefinition.getExtendedProperty().toByteArray());
    final DictionaryEncodedColumns dictionaryEncodedColumns = xAttr.getDictionaryEncodedColumns();
    if (dictionaryEncodedColumns != null) {
      dictionaryVersion = dictionaryEncodedColumns.getVersion();
      // Construct paths to dictionary files based on the version found in namespace. Do NOT look for files during planning.
      final Path dictionaryRootPath = new Path(dictionaryEncodedColumns.getRootPath());
      for (String dictionaryEncodedColumn : dictionaryEncodedColumns.getColumnsList()) {
        if (!columnsPushedToScan.contains(dictionaryEncodedColumn)) {
          dictionaryEncodedColumnsToDictionaryFilePath.put(dictionaryEncodedColumn,
            GlobalDictionaryBuilder.dictionaryFilePath(dictionaryRootPath, dictionaryEncodedColumn).toString());
        }
      }
    }

    if (dictionaryEncodedColumnsToDictionaryFilePath.isEmpty()) {
      return new PrelWithDictionaryInfo(parquetScanPrel);
    }

    final StoragePluginId storagePluginId = parquetScanPrel.getPluginId();
    boolean encodedColumns = false;
    final List<RelDataTypeField> newFields = Lists.newArrayList();
    final GlobalDictionaryFieldInfo[] fieldInfos = new GlobalDictionaryFieldInfo[parquetScanPrel.getRowType().getFieldCount()];
    final List<GlobalDictionaryFieldInfo> globalDictionaryColumns = Lists.newArrayList();

    for (int i = 0; i < parquetScanPrel.getRowType().getFieldCount(); ++i) {
      final RelDataTypeField field = parquetScanPrel.getRowType().getFieldList().get(i);
      if (dictionaryEncodedColumnsToDictionaryFilePath.containsKey(field.getName())) {
        fieldInfos[i] = new GlobalDictionaryFieldInfo(
          dictionaryVersion,
          field.getName(),
          storagePluginId,
          CompleteType.fromMinorType(TypeInferenceUtils.getMinorTypeFromCalciteType(field.getType())).getType(),
          dictionaryEncodedColumnsToDictionaryFilePath.get(field.getName()),
          new RelDataTypeFieldImpl(field.getName(), field.getIndex(), field.getType()));
        newFields.add(dictionaryEncodedField(field));
        globalDictionaryColumns.add(fieldInfos[i]);
        encodedColumns = true;
      } else {
        fieldInfos[i] = null;
        newFields.add(field);
      }
    }

    if (!encodedColumns) {
      return new PrelWithDictionaryInfo(parquetScanPrel);
    }

    final RelDataType newRelDataType = PrelWithDictionaryInfo.toRowDataType(newFields, parquetScanPrel.getCluster().getTypeFactory());
    final ParquetScanPrel newParquetScanPrel = parquetScanPrel.cloneWithGlobalDictionaryColumns(globalDictionaryColumns, newRelDataType);
    return new PrelWithDictionaryInfo(newParquetScanPrel, fieldInfos);
  }

  private static class InputReferenceRexVisitor extends RexShuttle {
    private final Set<Integer> fieldsUsed;

    InputReferenceRexVisitor(Set<Integer> fieldsUsed) {
      this.fieldsUsed = fieldsUsed;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      fieldsUsed.add(inputRef.getIndex());
      return super.visitInputRef(inputRef);
    }
  }

}

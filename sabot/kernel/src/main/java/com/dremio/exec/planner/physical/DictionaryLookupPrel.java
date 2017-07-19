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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.DictionaryLookupPOP;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.Maps;


/**
 * Convert dictionary ids to original values
 */
public class DictionaryLookupPrel extends SinglePrel {

  private final Map<String, GlobalDictionaryFieldInfo> dictionaryEncodedFields;
  private final RelDataType relDataType;

  private static final String[] EMPTY_STRING_ARRAY = new String[0];

  private DictionaryLookupPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelDataType relDataType,
                               Map<String, GlobalDictionaryFieldInfo> dictionaryEncodedFields) {
    super(cluster, traits, child);
    this.relDataType = relDataType;
    this.dictionaryEncodedFields = dictionaryEncodedFields;
  }

  public DictionaryLookupPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
                              RelDataType relDataType,
                              List<GlobalDictionaryFieldInfo> globalDictionaryFieldInfoList) {
    super(cluster, traits, child);
    this.relDataType = relDataType;
    this.dictionaryEncodedFields = Maps.newHashMap();
    for (GlobalDictionaryFieldInfo fieldInfo : globalDictionaryFieldInfoList) {
      this.dictionaryEncodedFields.put(fieldInfo.getFieldName(), fieldInfo);
    }
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    // must have only one input
    return new DictionaryLookupPrel(getCluster(), traitSet, inputs.get(0), relDataType, dictionaryEncodedFields);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();
    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    DictionaryLookupPOP dictionaryLookupPOP = new DictionaryLookupPOP(creator.getContext().getStorage(), childPOP, dictionaryEncodedFields);
    return creator.addMetadata(this, dictionaryLookupPOP);
  }

  @Override
  protected RelDataType deriveRowType() {
    return relDataType;
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.NONE_AND_TWO;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return ((Prel)getInput()).getEncoding();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    final String[] fields = dictionaryEncodedFields.keySet().toArray(EMPTY_STRING_ARRAY);
    Arrays.sort(fields);
    return super.explainTerms(pw).item("decoded fields", Arrays.toString(fields));
  }
}

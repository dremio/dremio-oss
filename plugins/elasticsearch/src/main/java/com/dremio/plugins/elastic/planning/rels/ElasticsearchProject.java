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
package com.dremio.plugins.elastic.planning.rels;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.CompleteType;
import com.dremio.elastic.proto.ElasticReaderProto.ElasticSpecialType;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ProjectRelBase;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.plugins.elastic.ElasticsearchConf;
import com.dremio.plugins.elastic.ElasticsearchStoragePlugin;
import com.dremio.plugins.elastic.planning.rules.ProjectAnalyzer;
import com.dremio.plugins.elastic.planning.rules.SchemaField;

public class ElasticsearchProject extends ProjectRelBase implements ElasticsearchPrel {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticsearchProject.class);

  private final boolean needsScript;
  private final boolean scriptsEnabled;
  private final StoragePluginId pluginId;

  public ElasticsearchProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType, StoragePluginId pluginId) {
    super(Prel.PHYSICAL, cluster, traits, input, projects, rowType);
    this.needsScript = !MoreRelOptUtil.isSimpleColumnSelection(this);
    this.pluginId = pluginId;
    this.scriptsEnabled = ElasticsearchConf.createElasticsearchConf(pluginId.getConnectionConf()).isScriptsEnabled();
  }

  public boolean canPushdown(ElasticIntermediateScanPrel scan, FunctionLookupContext functionLookupContext, Set<ElasticSpecialType> disallowedSpecialTypes){
    ElasticsearchConf config = ElasticsearchConf.createElasticsearchConf(scan.getPluginId().getConnectionConf());
    final boolean scriptsEnabled = config.isScriptsEnabled();
    final boolean painlessAllowed = config.isUsePainless();
    final boolean supportsV5Features = pluginId.getCapabilities().getCapability(ElasticsearchStoragePlugin.ENABLE_V5_FEATURES);
    for (RexNode originalExpression : getProjects()) {
      try {
        final RexNode expr = SchemaField.convert(originalExpression, scan, disallowedSpecialTypes);
        ProjectAnalyzer.getScript(expr, painlessAllowed, supportsV5Features, scriptsEnabled, true,
            config.isAllowPushdownOnNormalizedOrAnalyzedFields(), false);
      } catch (Exception e) {
        logger.debug("Exception while attempting pushdown.", e);
        return false;
      }
    }

    final BatchSchema schema = ((ElasticsearchPrel) getInput()).getSchema(functionLookupContext);
    // We are not going to pushdown anything that returns union type!
    for (Field field : schema.getFields()) {
      final CompleteType type = CompleteType.fromField(field);
      if (type.isUnion()) {
        return false;
      }
    }

    return true;
  }

  public boolean isSelectStar() {
    RelOptTable inputTable = getInput().getTable();
    if (inputTable == null) {
      return false;
    }
    return MoreRelOptUtil.containIdentity(getProjects(),
      getRowType(),
      inputTable.getRowType(),
      String::compareTo);
  }

  public boolean getNeedsScript() {
    return needsScript;
  }

  @Override
  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (needsScript && !scriptsEnabled) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return super.computeSelfCost(planner, mq).multiplyBy(0.1D);
  }

  @Override
  public Project copy(RelTraitSet relTraitSet, RelNode relNode, List<RexNode> list, RelDataType relDataType) {
    return new ElasticsearchProject(this.getCluster(), relTraitSet, relNode, list, relDataType, pluginId);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> prelVisitor, X value) throws E {
    return prelVisitor.visitPrel(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public BatchSchema getSchema(FunctionLookupContext context) {
    final ElasticsearchPrel child = (ElasticsearchPrel) getInput();
    final BatchSchema childSchema = child.getSchema(context);
    ParseContext parseContext = new ParseContext(PrelUtil.getSettings(getCluster()));
    return ExpressionTreeMaterializer.materializeFields(getProjectExpressions(parseContext), childSchema, context)
        .setSelectionVectorMode(childSchema.getSelectionVectorMode())
        .build();
  }

  @Override
  public ScanBuilder newScanBuilder() {
    throw new IllegalStateException("This should never be on the edge.");
  }
}

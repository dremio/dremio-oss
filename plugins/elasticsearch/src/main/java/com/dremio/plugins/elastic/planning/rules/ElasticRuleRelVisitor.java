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
package com.dremio.plugins.elastic.planning.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import com.dremio.exec.planner.common.MoreRelOptUtil.SubsetRemover;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchFilter;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchProject;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchSample;

/**
 * Visits the elasticsearch subtree and collapses the tree.
 */
public abstract class ElasticRuleRelVisitor extends RelVisitor {
  protected RexNode filterExprs = null;
  protected List<RexNode> projectExprs = null;
  protected RelDataType projectDataType = null;
  protected RelNode child = null;
  protected List<ElasticsearchPrel> parents = new ArrayList<>();
  protected boolean continueToChildren = true;
  final protected RelNode input;

  public ElasticRuleRelVisitor(RelNode input) {
    this.input = input;
  }

  abstract public void processFilter(ElasticsearchFilter filter);
  abstract public void processProject(ElasticsearchProject project);
  abstract public void processSample(ElasticsearchSample node);

  public ElasticRuleRelVisitor go() {
    go(input.accept(new SubsetRemover(false)));
    assert child != null;
    return this;
  }

  public List<RexNode> getProjectExprs() {
    return projectExprs;
  }

  @Override
  public void visit(RelNode node, int ordinal, RelNode parent) {
    if (node instanceof ElasticsearchFilter) {
      processFilter((ElasticsearchFilter) node);
    } else if (node instanceof ElasticsearchProject) {
      processProject((ElasticsearchProject) node);
    } else if (node instanceof ElasticsearchSample) {
      processSample((ElasticsearchSample) node);
    } else {
      child = node;
      continueToChildren = false;
    }

    if (continueToChildren) {
      super.visit(node, ordinal, parent);
    }
  }

  public ElasticsearchPrel getConvertedTree() {
    ElasticsearchPrel subTree = (ElasticsearchPrel) this.child;


    if (filterExprs != null) {
      subTree = new ElasticsearchFilter(subTree.getCluster(), subTree.getTraitSet(), subTree, filterExprs, subTree.getPluginId());
    }

    if (projectExprs != null) {
      subTree = new ElasticsearchProject(subTree.getCluster(), subTree.getTraitSet(), subTree, projectExprs, projectDataType, subTree.getPluginId());
    }

    if (parents != null && !parents.isEmpty()) {
      ListIterator<ElasticsearchPrel> iterator = parents.listIterator(parents.size());
      while (iterator.hasPrevious()) {
        final ElasticsearchPrel parent = iterator.previous();
        subTree = (ElasticsearchPrel) parent.copy(parent.getTraitSet(), Collections.singletonList((RelNode) subTree));
      }
    }

    return subTree;
  }

}

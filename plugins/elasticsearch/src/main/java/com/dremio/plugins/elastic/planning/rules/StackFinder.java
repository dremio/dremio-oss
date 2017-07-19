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
package com.dremio.plugins.elastic.planning.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.google.common.collect.ImmutableList;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchPrel;

/**
 * Get the stack of Elastic operators
 */
public final class StackFinder {

  private StackFinder(){}

  public static List<ElasticsearchPrel> getStack(RelNode rel){
    rel = rel.accept(new MoreRelOptUtil.SubsetRemover(false));
    List<ElasticsearchPrel> stack = new ArrayList<>();
    outside: while(rel != null){
      if( !(rel instanceof ElasticsearchPrel) ){
        throw new IllegalStateException("Stack should only include ElasticPrels, but actually included " + rel.getClass().getName());
      }
      stack.add((ElasticsearchPrel) rel);
      List<RelNode> nodes = rel.getInputs();
      switch(nodes.size()){
      case 0:
        break outside;
      case 1:
        rel = nodes.get(0);
        break;
      default:
        throw new IllegalStateException("Elastic rels should be single input or no input.");
      }
    }

    return ImmutableList.copyOf(stack);
  }

}

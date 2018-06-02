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
package com.dremio.exec.planner.sql.handlers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.FlattenVisitors;
import com.google.common.collect.ImmutableList;

public class DremioFieldTrimmer extends RelFieldTrimmer {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioFieldTrimmer.class);

  private final RelBuilder builder;

  public static DremioFieldTrimmer of(RelOptCluster cluster) {
    RelBuilder builder = DremioRelFactories.CALCITE_LOGICAL_BUILDER.create(cluster, null);
    return new DremioFieldTrimmer(cluster, builder);
  }

  private DremioFieldTrimmer(RelOptCluster cluster, RelBuilder builder) {
    super(null, builder);
    this.builder = builder;
  }

  public TrimResult trimFields(
      ScanCrel crel,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {

    if(fieldsUsed.cardinality() == crel.getRowType().getFieldCount()) {
      return result(crel, Mappings.createIdentity(crel.getRowType().getFieldCount()));
    }

    if(fieldsUsed.cardinality() == 0) {
      // do something similar to dummy project but avoid using a scan field. This ensures the scan
      // does a skipAll operation rather than projectin a useless column.
      final RelOptCluster cluster = crel.getCluster();
      final Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, crel.getRowType().getFieldCount(), 1);
      final RexLiteral expr = cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO);
      builder.push(crel);
      builder.project(ImmutableList.<RexNode>of(expr), ImmutableList.of("DUMMY"));
      return result(builder.build(), mapping);
    }

    final List<SchemaPath> paths = new ArrayList<>();
    final Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION, crel.getRowType().getFieldCount(), fieldsUsed.cardinality());
    int index = 0;
    for(int i : fieldsUsed) {
      paths.add(SchemaPath.getSimplePath(crel.getRowType().getFieldList().get(i).getName()));
      m.set(i, index);
      index++;
    }

    // unfortunately, we can't yet use newCrel as it currently messes with reflections. As such, we'll create a project.
    // ScanCrel newCrel = crel.cloneWithProject(paths);
    // return result(newCrel, m);

    builder.push(crel);
    builder.project(builder.fields(fieldsUsed.asList()));
    RelNode rel = builder.build();
    return result(rel, m);
  }


  // Overridden until CALCITE-2260 is fixed.
  @Override
  public TrimResult trimFields(
      SetOp setOp,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    if(!setOp.all) {
      return result(setOp, Mappings.createIdentity(setOp.getRowType().getFieldCount()));
    }
    return super.trimFields(setOp, fieldsUsed, extraFields);
  }

  @Override
  public TrimResult trimFields(Project project, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    int count = FlattenVisitors.count(project.getProjects());
    if(count == 0) {
      return super.trimFields(project, fieldsUsed, extraFields);
    }

    // start by trimming based on super.


    // if there are no flatten, trim is fine.
    TrimResult result = super.trimFields(project, fieldsUsed, extraFields);

    if(result.left.getRowType().getFieldCount() != fieldsUsed.cardinality()) {
      // we got a partial trim, which we don't handle. Skip the optimization.
      return result(project, Mappings.createIdentity(project.getRowType().getFieldCount()));

    }
    final RelNode resultRel = result.left;
    final Mapping finalMapping = result.right;

    if(resultRel instanceof Project && FlattenVisitors.count(((Project) resultRel).getProjects()) == count){
      // flatten count didn't change.
      return result;
    }

    /*
     * Flatten count changed. To solve, we'll actually increase the required fields to include the
     * flattens and then put another project on top that drops the extra fields, returning the
     * previously generated mapping.
     */
    ImmutableBitSet.Builder flattenColumnsBuilder = ImmutableBitSet.builder();
    {
      int i =0;
      for(RexNode n : project.getProjects()) {
        try {
          if(fieldsUsed.get(i)) {
            continue;
          }

          if(!FlattenVisitors.hasFlatten(n)) {
            continue;
          }

          // we have a flatten in an unused field.
          flattenColumnsBuilder.set(i);

        } finally {
          i++;
        }
      }
    }

    ImmutableBitSet unreferencedFlattenProjects = flattenColumnsBuilder.build();

    if(unreferencedFlattenProjects.isEmpty()) {
      // this should be impossible. fall back to using the base case (no column trim) as it means we just optimize less.
      logger.info("Failure while trying to trim flatten expression. Expressions {}, Columns to trim to: {}", project.getProjects(), fieldsUsed);
      return result(project, Mappings.createIdentity(project.getRowType().getFieldCount()));
    }

    final ImmutableBitSet fieldsIncludingFlattens = fieldsUsed.union(unreferencedFlattenProjects);
    final TrimResult result2 = super.trimFields(project, fieldsIncludingFlattens, extraFields);

    List<RexNode> finalProj = new ArrayList<>();
    builder.push(result2.left);

    int i = 0;
    for(int index : fieldsIncludingFlattens) {
      if(fieldsUsed.get(index)) {
        finalProj.add(builder.field(i));
      }
      i++;
    }


    // drop the flatten columns in a subsequent projection.
    return result(builder.project(finalProj).build(), finalMapping);

  }

}

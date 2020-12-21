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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.planner.common.ScreenRelBase;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class ScreenPrel extends ScreenRelBase implements Prel, HasDistributionAffinity {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.screen.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.screen.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  public static final String MIXED_TYPES_ERROR = "Mixed types are no longer supported as returned values over JDBC, ODBC and Flight connections.";

  public ScreenPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
    super(Prel.PHYSICAL, cluster, traits, child);
  }

  @Override
  public ScreenPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScreenPrel(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPop = child.getPhysicalOperator(creator);
    BatchSchema schema = childPop.getProps().getSchema();
    checkForUnion(schema.getFields());

    return new Screen(
        creator.props(this, null, schema, RESERVE, LIMIT),
        childPop);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitScreen(this, value);
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
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.NONE;
  }

  private static void checkForUnion(List<Field> fields) {
    for(Field f : fields) {
      if(f.getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.Union) {
        throw UserException.unsupportedError().message(MIXED_TYPES_ERROR
          + " Cast column \"%s\" to a primitive data type either in the "
          + "select statement or the VDS definition.", f.getName()).buildSilently();
      } else {
        checkForUnion(f.getChildren());
      }
    }
  }
}

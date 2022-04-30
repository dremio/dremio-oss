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
package com.dremio.exec.planner.fragment;

import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpWithMinorSpecificAttrs;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.google.common.collect.Lists;

/*
 * Visitor to populate all minor-specific attributes from each operator.
 */
public class MinorDataPopulator extends AbstractPhysicalVisitor<PhysicalOperator, MinorDataReader, ExecutionSetupException> {
  private static final MinorDataPopulator INSTANCE = new MinorDataPopulator();

  public MinorDataPopulator() {
  }

  public static PhysicalOperator populate(
    FragmentHandle handle,
    PhysicalOperator root,
    MinorDataSerDe serDe,
    MinorAttrsMap map,
    PlanFragmentsIndex index)
      throws ExecutionSetupException {

    // Create an instance of the reader for de-serializing the attributes.
    MinorDataReader reader = new MinorDataReader(handle, serDe, index, map);

    // walk the tree and populate attributes.
    return root.accept(INSTANCE, reader);
  }

  @Override
  public PhysicalOperator visitExchange(Exchange exchange, MinorDataReader reader) throws ExecutionSetupException {
    throw new ExecutionSetupException(
      "Exchange type operator not expected in serialized plan, found " + exchange.getClass().getCanonicalName());
  }

  @SuppressWarnings("unchecked")
  @Override
  public PhysicalOperator visitGroupScan(@SuppressWarnings("rawtypes") GroupScan groupScan, MinorDataReader reader) throws ExecutionSetupException {
    throw new ExecutionSetupException(
      "GroupScan type operator not expected in serialized plan, found " + groupScan.getClass().getCanonicalName());
  }

  @Override
  public PhysicalOperator visitOp(PhysicalOperator op, MinorDataReader reader)  throws ExecutionSetupException {
    // create children and populate their attributes.
    List<PhysicalOperator> children = Lists.newArrayList();
    for (PhysicalOperator child : op){
      children.add(child.accept(this, reader));
    }

    // create this operator, and populate it's attributes.
    PhysicalOperator newOp = op.getNewWithChildren(children);
    try {
      if (newOp instanceof OpWithMinorSpecificAttrs) {
        assert op != newOp;

        ((OpWithMinorSpecificAttrs)newOp).populateMinorSpecificAttrs(reader);
      }
    } catch (Exception e) {
      throw new ExecutionSetupException("Failed to populate minor specific attributes", e);
    }
    return newOp;
  }

}

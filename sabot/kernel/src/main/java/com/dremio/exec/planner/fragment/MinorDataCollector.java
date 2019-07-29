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

import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.OpWithMinorSpecificAttrs;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.CoordExecRPC.MinorAttr;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.work.foreman.ForemanSetupException;

/*
 * Visitor to collect all minor-specific attributes from each operator.
 */
public class MinorDataCollector
    extends AbstractPhysicalVisitor<Void, MinorDataWriter, ForemanSetupException> {
  private static final MinorDataCollector INSTANCE = new MinorDataCollector();

  public MinorDataCollector() {}

  public static List<MinorAttr> collect(
    FragmentHandle handle,
    NodeEndpoint endpoint,
    PhysicalOperator root,
    MinorDataSerDe serDe,
    PlanFragmentsIndex.Builder indexBuilder)
      throws ForemanSetupException {

    // Create an instance of the writer for serializing the attributes.
    MinorDataWriter writer = new MinorDataWriter(handle, endpoint, serDe, indexBuilder);

    // walk the tree and collect attributes.
    root.accept(INSTANCE, writer);

    // return the collected attributes.
    return writer.getAllAttrs();
  }

  @Override
  public Void visitOp(PhysicalOperator op, MinorDataWriter writer) throws ForemanSetupException {
    try {
      // collect attributes for this operator.
      if (op instanceof OpWithMinorSpecificAttrs) {
        ((OpWithMinorSpecificAttrs)op).collectMinorSpecificAttrs(writer);
      }
    } catch (Exception e) {
      throw new ForemanSetupException("Failed to collect minor specific data", e);
    }

    // visit the children of the operator.
    return super.visitChildren(op, writer);
  }
}

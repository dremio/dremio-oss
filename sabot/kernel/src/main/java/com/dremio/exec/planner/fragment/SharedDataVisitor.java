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
package com.dremio.exec.planner.fragment;

import java.util.ArrayList;
import java.util.List;

import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.CoordExecRPC.SharedData;
import com.dremio.exec.work.foreman.ForemanSetupException;

/**
 * Responsible for collecting the shared data from all operators
 */
public class SharedDataVisitor extends AbstractPhysicalVisitor<Void, List<SharedData>, ForemanSetupException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SharedDataVisitor.class);

  private SharedDataVisitor() {}

  public static List<SharedData> collect(PhysicalOperator root) throws ForemanSetupException {
    SharedDataVisitor sharedDataVisitor = new SharedDataVisitor();
    List<SharedData> sharedData = new ArrayList<>();
    root.accept(sharedDataVisitor, sharedData);
    return sharedData;
  }

  @Override
  public Void visitOp(PhysicalOperator op, List<SharedData> sharedData)  throws ForemanSetupException {
    int opId = op.getOperatorId() & Short.MAX_VALUE;
    int fragId = op.getOperatorId() >> 16;
    op.getSharedData().forEach(
      s -> sharedData.add(SharedData.newBuilder()
        .setName(s.getKey())
        .setValue(s.getValue())
        .setMajorFragmentId(fragId)
        .setOperatorId(opId)
        .build()));

    for (PhysicalOperator child : op) {
      child.accept(this, sharedData);
    }
    return null;
  }
}

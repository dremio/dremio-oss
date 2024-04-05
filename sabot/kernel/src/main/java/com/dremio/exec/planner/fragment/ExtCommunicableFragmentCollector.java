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

import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.Exchange;
import com.dremio.exec.physical.base.PhysicalOperator;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Visitor for collecting external communicable fragment information across all operators within a
 * fragment.
 */
public class ExtCommunicableFragmentCollector
    extends AbstractPhysicalVisitor<Set<Integer>, Wrapper, RuntimeException> {

  @Override
  public Set<Integer> visitExchange(Exchange exchange, Wrapper node) {
    if (node.getNode().getSendingExchange() != exchange) {
      // Stop at receiving exchange. Major fragment will change after this stage.
      return Collections.emptySet();
    }

    final Set<Integer> extCommunicableFragments =
        new HashSet<>(
            Optional.ofNullable(exchange.getExtCommunicableMajorFragments())
                .orElse(Collections.emptySet()));
    extCommunicableFragments.addAll(exchange.getChild().accept(this, node));
    return extCommunicableFragments;
  }

  @Override
  public Set<Integer> visitOp(PhysicalOperator op, Wrapper node) {
    final Set<Integer> extCommunicableFragments =
        new HashSet<>(
            Optional.ofNullable(op.getExtCommunicableMajorFragments())
                .orElse(Collections.emptySet()));
    for (PhysicalOperator child : op) {
      extCommunicableFragments.addAll(child.accept(this, node));
    }
    return extCommunicableFragments;
  }
}

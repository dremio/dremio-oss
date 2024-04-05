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
package com.dremio.exec.planner.physical.visitor;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.physical.BridgeExchangePrel;
import com.dremio.exec.planner.physical.BridgeReaderPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;

public class BridgeReaderSchemaFinder {
  public static Prel findAndSetSchemas(RelNode root, QueryContext context) {
    Map<String, BatchSchema> schemaMap = new HashMap<>();
    Map<String, BridgeExchangePrel> bridgeExchangePrelMap = new HashMap<>();
    findBridgeExchanges(root, bridgeExchangePrelMap);
    return findAndSetSchemas(root, schemaMap, bridgeExchangePrelMap, context);
  }

  private static void findBridgeExchanges(
      RelNode rel, Map<String, BridgeExchangePrel> bridgeExchangePrelMap) {
    for (RelNode child : rel.getInputs()) {
      findBridgeExchanges(child, bridgeExchangePrelMap);
    }
    if (rel instanceof BridgeExchangePrel) {
      bridgeExchangePrelMap.put(
          ((BridgeExchangePrel) rel).getBridgeSetId(), ((BridgeExchangePrel) rel));
    }
  }

  private static Prel findAndSetSchemas(
      RelNode rel,
      Map<String, BatchSchema> schemaMap,
      Map<String, BridgeExchangePrel> bridgeExchangeMap,
      QueryContext context) {
    List<RelNode> children = new ArrayList<>();
    for (RelNode child : rel.getInputs()) {
      children.add(findAndSetSchemas(child, schemaMap, bridgeExchangeMap, context));
    }
    if (rel instanceof BridgeExchangePrel) {
      BridgeExchangePrel bridgeExchangePrel =
          (BridgeExchangePrel) rel.copy(rel.getTraitSet(), children);
      schemaMap.put(bridgeExchangePrel.getBridgeSetId(), lookupSchema(bridgeExchangePrel, context));
      bridgeExchangeMap.put(bridgeExchangePrel.getBridgeSetId(), bridgeExchangePrel);
      return bridgeExchangePrel;
    }
    if (rel instanceof BridgeReaderPrel) {
      BridgeReaderPrel bridgeReaderPrel = (BridgeReaderPrel) rel;
      BatchSchema schema = schemaMap.get(bridgeReaderPrel.getBridgeSetId());
      if (schema != null) {
        return bridgeReaderPrel.copyWithSchema(schema);
      }
      BridgeExchangePrel bridgeExchangePrel =
          bridgeExchangeMap.get(bridgeReaderPrel.getBridgeSetId());
      Preconditions.checkNotNull(bridgeExchangePrel);
      findAndSetSchemas(bridgeExchangePrel, schemaMap, bridgeExchangeMap, context);
      schema = schemaMap.get(bridgeReaderPrel.getBridgeSetId());
      Preconditions.checkNotNull(schema);
      return bridgeReaderPrel.copyWithSchema(schema);
    }
    return (Prel) rel.copy(rel.getTraitSet(), children);
  }

  private static BatchSchema lookupSchema(Prel prel, QueryContext context) {
    // This is need to get row type for execution, we do not have calcite to arrow row type
    // conversion.
    try {
      PhysicalPlanCreator planCreator =
          new PhysicalPlanCreator(context, PrelSequencer.getIdMap(prel));
      return prel.getPhysicalOperator(planCreator).getProps().getSchema();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

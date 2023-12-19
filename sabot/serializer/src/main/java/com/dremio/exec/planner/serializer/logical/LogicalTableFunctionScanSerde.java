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
package com.dremio.exec.planner.serializer.logical;

import java.util.stream.Collectors;

import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;

import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PLogicalTableFunctionScan;
import com.dremio.plan.serialization.PRelColumnMapping;

/**
 * Serde for LogicalTableFunctionScan
 */
public final class LogicalTableFunctionScanSerde
  implements RelNodeSerde<LogicalTableFunctionScan, PLogicalTableFunctionScan> {
  @Override
  public PLogicalTableFunctionScan serialize(LogicalTableFunctionScan node, RelToProto s) {
    PLogicalTableFunctionScan.Builder builder = PLogicalTableFunctionScan.newBuilder()
      .addAllInputs(node.getInputs().stream().map(s::toProto).collect(Collectors.toList()))
      .setRexCall(s.toProto(node.getCall()))
      .setRowType(s.toProto(node.getRowType()));

    if (node.getColumnMappings() != null) {
      builder.addAllColumnMappings(
        node.getColumnMappings()
          .stream()
          .map(RelColumnMappingSerde::toProto)
          .collect(Collectors.toList()));
    }

    return builder.build();
  }

  @Override
  public LogicalTableFunctionScan deserialize(
    PLogicalTableFunctionScan pLogicalTableFunctionScan,
    RelFromProto s) {
    return LogicalTableFunctionScan.create(
      s.cluster(),
      pLogicalTableFunctionScan
        .getInputsList()
        .stream()
        .map(s::toRel)
        .collect(Collectors.toList()),
      s.toRex(pLogicalTableFunctionScan.getRexCall()),
      null,
      s.toRelDataType(pLogicalTableFunctionScan.getRowType()),
      pLogicalTableFunctionScan
        .getColumnMappingsList()
        .stream()
        .map(RelColumnMappingSerde::fromProto)
        .collect(Collectors.toSet()));
  }

  private static final class RelColumnMappingSerde {
    public static PRelColumnMapping toProto(RelColumnMapping relColumnMapping) {
      return PRelColumnMapping.newBuilder()
        .setIInputColumn(relColumnMapping.iInputColumn)
        .setIInputRel(relColumnMapping.iInputRel)
        .setIInputColumn(relColumnMapping.iInputRel)
        .setDerived(relColumnMapping.derived)
        .build();
    }

    public static RelColumnMapping fromProto(PRelColumnMapping pRelColumnMapping) {
      return new RelColumnMapping(
        pRelColumnMapping.getIOutputColumn(),
        pRelColumnMapping.getIInputRel(),
        pRelColumnMapping.getIInputColumn(),
        pRelColumnMapping.getDerived());
    }
  }
}

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

import com.dremio.exec.planner.serializer.RelNodeSerde;
import com.dremio.plan.serialization.PLogicalValueRecord;
import com.dremio.plan.serialization.PLogicalValues;
import com.dremio.plan.serialization.PRelDataTypeField;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;

/** Serde for LogicalValues */
public final class LogicalValuesSerde implements RelNodeSerde<LogicalValues, PLogicalValues> {
  @Override
  public PLogicalValues serialize(LogicalValues values, RelToProto s) {
    return PLogicalValues.newBuilder()
        .addAllFields(
            values.getRowType().getFieldList().stream()
                .map(f -> toProto(f, s))
                .collect(Collectors.toList()))
        .addAllRecord(
            values.tuples.stream()
                .map(
                    c -> {
                      return PLogicalValueRecord.newBuilder()
                          .addAllLiteral(
                              c.asList().stream().map(s::toProto).collect(Collectors.toList()))
                          .build();
                    })
                .collect(Collectors.toList()))
        .build();
  }

  @Override
  public LogicalValues deserialize(PLogicalValues node, RelFromProto s) {
    return (LogicalValues)
        s.builder()
            .values(
                node.getRecordList().stream()
                    .map(r -> toLiterals(r, s))
                    .collect(Collectors.toList()),
                fromProto(node.getFieldsList(), s))
            .build();
  }

  private List<RexLiteral> toLiterals(PLogicalValueRecord record, RelFromProto s) {
    return record.getLiteralList().stream()
        .map(s::toRex)
        .map(l -> ((RexLiteral) l))
        .collect(Collectors.toList());
  }

  private PRelDataTypeField toProto(RelDataTypeField f, RelToProto s) {
    return PRelDataTypeField.newBuilder()
        .setName(f.getName())
        .setIndex(f.getIndex())
        .setType(s.toProto(f.getType()))
        .build();
  }

  private RelDataType fromProto(List<PRelDataTypeField> fields, RelFromProto s) {
    return s.builder()
        .getTypeFactory()
        .createStructType(
            fields.stream().map(t -> s.toRelDataType(t.getType())).collect(Collectors.toList()),
            fields.stream().map(PRelDataTypeField::getName).collect(Collectors.toList()));
  }
}

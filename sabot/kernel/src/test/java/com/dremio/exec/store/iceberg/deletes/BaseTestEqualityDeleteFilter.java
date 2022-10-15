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
package com.dremio.exec.store.iceberg.deletes;

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;
import static com.dremio.sabot.op.common.ht2.LBlockHashTable.ORDINAL_SIZE;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Generator;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;

public class BaseTestEqualityDeleteFilter extends BaseTestOperator {

  protected EqualityDeleteHashTable buildTable(RecordSet buildRs, List<Integer> buildOrdinals) throws Exception {
    int buildBatchSize = buildRs.getMaxBatchSize();
    int buildRecords = buildRs.getTotalRecords();

    try (Generator buildGenerator = buildRs.toGenerator(getTestAllocator());
        ArrowBuf buildOrdinalBuf = getTestAllocator().buffer((long) buildBatchSize * ORDINAL_SIZE)) {

      List<SchemaPath> equalityFields = getFields(buildRs.getSchema());
      VectorAccessible buildAccessible = buildGenerator.getOutput();
      EqualityDeleteHashTable.Builder builder = new EqualityDeleteHashTable.Builder(getTestAllocator(),
          equalityFields, getFieldVectors(buildAccessible, buildRs.getSchema()), buildRecords, buildBatchSize);

      int records;
      while ((records = buildGenerator.next(buildBatchSize)) > 0) {
        builder.insertBatch(records, buildOrdinalBuf);
        if (buildOrdinals != null) {
          appendOrdinalsToList(buildOrdinalBuf, records, buildOrdinals);
        }
      }

      return builder.build();
    }
  }

  protected PivotDef createPivotDef(VectorAccessible accessible, List<SchemaPath> equalityFields) {
    List<FieldVectorPair> fieldVectorPairs = equalityFields.stream()
        .map(f -> (FieldVector) getVectorFromSchemaPath(accessible, f.getRootSegment().getPath()))
        .map(v -> new FieldVectorPair(v, v))
        .collect(Collectors.toList());

    return PivotBuilder.getBlockDefinition(fieldVectorPairs);
  }

  protected void find(EqualityDeleteHashTable table, int records, PivotDef pivotDef, ArrowBuf outOrdinals) {
    try (FixedBlockVector fbv = new FixedBlockVector(getTestAllocator(), pivotDef.getBlockWidth());
        VariableBlockVector vbv = new VariableBlockVector(getTestAllocator(), pivotDef.getVariableCount())) {
      Pivots.pivot(pivotDef, records, fbv, vbv);
      table.find(records, fbv, vbv, outOrdinals);
    }
  }

  protected List<SchemaPath> getFields(BatchSchema schema) {
    return schema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList());
  }

  protected List<FieldVector> getFieldVectors(VectorAccessible accessible, BatchSchema schema) {
    return schema.getFields().stream()
        .map(f -> (FieldVector) getVectorFromSchemaPath(accessible, f.getName()))
        .collect(Collectors.toList());
  }

  protected void appendOrdinalsToList(ArrowBuf ordinalBuf, int count, List<Integer> ordinals) {
    for (int i = 0; i < count; i++) {
      ordinals.add(ordinalBuf.getInt((long) i * ORDINAL_SIZE));
    }
  }
}

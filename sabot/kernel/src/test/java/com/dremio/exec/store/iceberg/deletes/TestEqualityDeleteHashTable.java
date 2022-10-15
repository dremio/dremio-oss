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

import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rb;
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.op.common.ht2.LBlockHashTable.ORDINAL_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.Generator;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.google.common.collect.ImmutableList;

public class TestEqualityDeleteHashTable extends BaseTestEqualityDeleteFilter {

  private static final BatchSchema SCHEMA_FIXED = BatchSchema.newBuilder()
      .addField(Field.nullable("col1", Types.MinorType.INT.getType()))
      .build();
  private static final BatchSchema SCHEMA_VAR = BatchSchema.newBuilder()
      .addField(Field.nullable("col1", Types.MinorType.VARCHAR.getType()))
      .build();
  private static final BatchSchema SCHEMA_FIXED_AND_VAR = BatchSchema.newBuilder()
      .addField(Field.nullable("col1", Types.MinorType.INT.getType()))
      .addField(Field.nullable("col2", Types.MinorType.VARCHAR.getType()))
      .build();

  @Test
  public void testSingleFixedField() throws Exception {
    RecordSet build = rs(SCHEMA_FIXED,
        rb(r(1), r(5), r(2)),
        rb(r((Integer) null), r(-8), r(10)));
    RecordSet probe = rs(SCHEMA_FIXED,
        rb(r(50), r((Integer) null), r(-8)),
        rb(r(10), r(1), r(3)),
        rb(r(2), r(-8)));

    // expected build index mapping for probe lookups
    List<Integer> expectedOrdinalMapping = ImmutableList.of(-1, 3, 4, 5, 0, -1, 2, 4);

    buildAndValidate(build, probe, 6, expectedOrdinalMapping);
  }

  @Test
  public void testSingleVarField() throws Exception {
    RecordSet build = rs(SCHEMA_VAR,
        rb(r("red"), r("orange"), r("blue")),
        rb(r("green"), r("purple"), r((String) null)));
    RecordSet probe = rs(SCHEMA_VAR,
        rb(r("purple"), r("magenta"), r("red")),
        rb(r("green"), r("white"), r("purple")),
        rb(r((String) null), r("orange"), r("cyan")));

    // expected build index mapping for probe lookups
    List<Integer> expectedOrdinalMapping = ImmutableList.of(4, -1, 0, 3, -1, 4, 5, 1, -1);

    buildAndValidate(build, probe, 6, expectedOrdinalMapping);
  }

  @Test
  public void testFixedAndVarFields() throws Exception {
    RecordSet build = rs(SCHEMA_FIXED_AND_VAR,
        rb(r(1, "red"), r(5, "orange"), r(2, "blue")),
        rb(r(null, "green"), r(-8, "purple"), r(10, null)),
        rb(r(null, null)));
    RecordSet probe = rs(SCHEMA_FIXED_AND_VAR,
        rb(r(-8, "purple"), r(null, "magenta"), r(null, null)),
        rb(r(null, "green"), r(5, "magenta"), r(-8, "purple")),
        rb(r(5, "orange"), r(10, "cyan"), r(2, "blue")));

    // expected build index mapping for probe lookups
    List<Integer> expectedOrdinalMapping = ImmutableList.of(4, -1, 6, 3, -1, 4, 1, -1, 2);

    buildAndValidate(build, probe, 7, expectedOrdinalMapping);
  }

  private void buildAndValidate(RecordSet buildRs, RecordSet probeRs, int expectedSize,
      List<Integer> expectedOrdinalMapping)
      throws Exception {
    int buildBatchSize = buildRs.getMaxBatchSize();
    int buildRecords = buildRs.getTotalRecords();
    int probeBatchSize = probeRs.getMaxBatchSize();

    List<Integer> buildOrdinals = new ArrayList<>();
    List<Integer> probeOrdinals = new ArrayList<>();

    try (EqualityDeleteHashTable table = buildTable(buildRs, buildOrdinals);
        Generator probeGenerator = probeRs.toGenerator(getTestAllocator());
        ArrowBuf probeOrdinalBuf = getTestAllocator().buffer((long) probeBatchSize * ORDINAL_SIZE)) {

      assertThat(table.size()).isEqualTo(expectedSize);

      VectorAccessible probeAccessible = probeGenerator.getOutput();
      PivotDef probePivot = createPivotDef(probeAccessible, table.getEqualityFields());

      int records;
      while ((records = probeGenerator.next(probeBatchSize)) > 0) {
        find(table, records, probePivot, probeOrdinalBuf);
        appendOrdinalsToList(probeOrdinalBuf, records, probeOrdinals);
      }
    }

    List<Integer> actualOrdinalMapping = probeOrdinals.stream()
        .map(i -> i == -1 ? -1 : buildOrdinals.indexOf(i))
        .collect(Collectors.toList());

    assertThat(actualOrdinalMapping).isEqualTo(expectedOrdinalMapping);
  }
}

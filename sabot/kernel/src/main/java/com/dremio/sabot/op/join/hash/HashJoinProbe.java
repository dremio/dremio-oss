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

package com.dremio.sabot.op.join.hash;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.common.hashtable.HashTable;
import java.util.BitSet;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.calcite.rel.core.JoinRelType;

public interface HashJoinProbe {
  public static TemplateClassDefinition<HashJoinProbe> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<HashJoinProbe>(HashJoinProbe.class, HashJoinProbeTemplate.class);

  /* The probe side of the hash join can be in the following two states
   * 1. PROBE_PROJECT: Inner join case, we probe our hash table to see if we have a
   *    key match and if we do we project the record
   * 2. PROJECT_RIGHT: Right Outer or Full Outer joins where we are projecting the records
   *    from the build side that did not match any records on the probe side. For Left outer
   *    case we handle it internally by projecting the record if there isn't a match on the build side
   * 3. DONE: Once we have projected all possible records we are done
   */
  public static enum ProbeState {
    PROBE_PROJECT,
    PROJECT_RIGHT,
    DONE
  }

  void setupHashJoinProbe(
      FunctionContext functionContext,
      VectorAccessible buildBatch,
      VectorAccessible probeBatch,
      VectorAccessible outgoing,
      HashTable hashTable,
      JoinRelType joinRelType,
      List<BuildInfo> buildInfos,
      List<ArrowBuf> startIndices,
      List<BitSet> keyMatchBitVectors,
      int maxHashTableIndex,
      int targetRecordsPerBatch);

  /**
   * Project any remaining build items that were not matched. Only used when doing a FULL or RIGHT
   * join.
   *
   * @return Negative output if records were output but batch wasn't completed. Postive output if
   *     batch was completed.
   */
  int projectBuildNonMatches();

  /**
   * Probe with current batch.
   *
   * @return number of records matched. negative if we failed to output all records.
   */
  int probeBatch();
}

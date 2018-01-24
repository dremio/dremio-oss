/*
 * Copyright (C) 2017 Dremio Corporation
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
/*
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
package io.airlift.tpch;

import com.google.common.base.Charsets;
import io.airlift.tpch.GenerationDefinition.TpchTable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableVarCharVector;

public class NationGenerator extends TpchGenerator {
  private static final int COMMENT_AVERAGE_LENGTH = 72;

  private final Distribution nations;

  private final NullableBigIntVector nationKey;
  private final NullableVarCharVector name;
  private final NullableBigIntVector regionKey;
  private final NullableVarCharVector comment;

  final RandomText commentRandom = randomText(606179079, TextPool.getDefaultTestPool(), COMMENT_AVERAGE_LENGTH);

  public NationGenerator(BufferAllocator allocator, GenerationDefinition def, String...includedColumns) {
    super(TpchTable.NATION, allocator, def, 1, includedColumns);
    this.nations = Distributions.getDefaultDistributions().getNations();

    this.nationKey = int8("n_nationKey");
    this.name = varChar("n_name");
    this.regionKey = int8("n_regionKey");
    this.comment = varChar("n_comment");

    finalizeSetup();
  }

  @Override
  protected void generateRecord(long globalRecordIndex, int outputIndex) {

    nationKey.setSafe(outputIndex, globalRecordIndex);

    byte[] nameVal = nations.getValue((int) globalRecordIndex).getBytes(Charsets.UTF_8);
    name.setSafe(outputIndex, nameVal, 0, nameVal.length);

    regionKey.setSafe(outputIndex, nations.getWeight((int) globalRecordIndex));

    // comment
    byte[] commentVal = commentRandom.nextValue().getBytes(Charsets.UTF_8);
    comment.setSafe(outputIndex, commentVal, 0, commentVal.length);

  }

}

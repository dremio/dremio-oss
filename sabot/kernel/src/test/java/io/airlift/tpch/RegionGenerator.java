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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;

import io.airlift.tpch.GenerationDefinition.TpchTable;

public class RegionGenerator extends TpchGenerator {
  private static final int COMMENT_AVERAGE_LENGTH = 72;

  private final BigIntVector regionKey;
  private final VarCharVector name;
  private final VarCharVector comment;

  private final Distribution regions;
  private final RandomText commentRandom;

  public RegionGenerator(BufferAllocator allocator, GenerationDefinition def, String...includedColumns) {
    super(TpchTable.REGION, allocator, def, 1, includedColumns);
    this.regions = Distributions.getDefaultDistributions().getRegions();
    this.commentRandom = randomText(1500869201, TextPool.getDefaultTestPool(), COMMENT_AVERAGE_LENGTH);

    this.regionKey = int8("r_regionKey");
    this.name = varChar("r_name");
    this.comment = varChar("r_comment");

    finalizeSetup();
  }

  @Override
  protected void generateRecord(long globalRecordIndex, int outputIndex) {

    regionKey.setSafe(outputIndex, globalRecordIndex);

    byte[] nameVal = regions.getValue((int) globalRecordIndex).getBytes(UTF_8);
    name.setSafe(outputIndex, nameVal, 0, nameVal.length);

    // comment
    byte[] commentVal = commentRandom.nextValue().getBytes(UTF_8);
    comment.setSafe(outputIndex, commentVal, 0, commentVal.length);

  }



}

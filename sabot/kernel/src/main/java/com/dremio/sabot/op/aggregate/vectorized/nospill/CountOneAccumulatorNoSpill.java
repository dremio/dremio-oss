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
package com.dremio.sabot.op.aggregate.vectorized.nospill;

import com.dremio.sabot.op.common.ht2.LBlockHashTableNoSpill;
import io.netty.util.internal.PlatformDependent;
import org.apache.arrow.vector.FieldVector;

public class CountOneAccumulatorNoSpill extends BaseSingleAccumulatorNoSpill {

  public CountOneAccumulatorNoSpill(FieldVector output) {
    super(null, output);
  }

  @Override
  public void accumulate(final long offsetAddr, final int count) {
    final long maxAddr = offsetAddr + count * 4;
    for (long ordinalAddr = offsetAddr; ordinalAddr < maxAddr; ordinalAddr += 4) {
      final int tableIndex = PlatformDependent.getInt(ordinalAddr);
      final long countAddr =
          getValueAddress(tableIndex >>> LBlockHashTableNoSpill.BITS_IN_CHUNK)
              + (tableIndex & LBlockHashTableNoSpill.CHUNK_OFFSET_MASK) * 8;
      PlatformDependent.putLong(countAddr, PlatformDependent.getLong(countAddr) + 1);
    }
  }
}

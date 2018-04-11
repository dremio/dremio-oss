/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.op.aggregate.vectorized;

import org.apache.arrow.vector.FieldVector;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;

import io.netty.util.internal.PlatformDependent;

public class CountColumnAccumulator extends BaseSingleAccumulator {

  public CountColumnAccumulator(FieldVector input, FieldVector output) {
    super(input, output);
  }

  public void accumulate(final long offsetAddr, final int count){
    final long maxAddr = offsetAddr + count * 4;
    final long incomingBit = getInput().getFieldBuffers().get(0).memoryAddress();
    final long[] valueAddresses = this.valueAddresses;

    int incomingIndex = 0;

    for(long ordinalAddr = offsetAddr; ordinalAddr < maxAddr; ordinalAddr += 4, incomingIndex++){
      final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
      final int tableIndex = PlatformDependent.getInt(ordinalAddr);
      final long countAddr = valueAddresses[tableIndex >>> LBlockHashTable.BITS_IN_CHUNK] + (tableIndex & LBlockHashTable.CHUNK_OFFSET_MASK) * 8;
      PlatformDependent.putLong(countAddr, PlatformDependent.getLong(countAddr) + bitVal);
    }

  }

}

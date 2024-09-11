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

package com.dremio.sabot.op.aggregate.vectorized.arrayagg;

import com.dremio.sabot.op.aggregate.vectorized.DecimalAccumulatorUtils;
import java.math.BigDecimal;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;

public final class BigDecimalArrayAggAccumulator extends ArrayAggAccumulator<BigDecimal> {

  private final byte[] valueBuffer = new byte[DecimalVector.TYPE_WIDTH];

  public BigDecimalArrayAggAccumulator(
      FieldVector input,
      FieldVector transferVector,
      BaseValueVector tempAccumulatorHolder,
      BufferAllocator allocator,
      int maxFieldSizeBytes,
      int initialVectorSize) {
    super(
        input,
        transferVector,
        tempAccumulatorHolder,
        allocator,
        maxFieldSizeBytes,
        initialVectorSize);
  }

  @Override
  protected int getFieldWidth() {
    return DecimalVector.TYPE_WIDTH;
  }

  @Override
  protected void writeItem(UnionListWriter writer, BigDecimal item) {
    writer.writeDecimal(item);
  }

  @Override
  protected ArrayAggAccumulatorHolder<BigDecimal> getAccumulatorHolder(
      int maxFieldSizeBytes, BufferAllocator allocator, int initialCapacity) {
    return new BigDecimalArrayAggAccumulatorHolder(
        allocator, (DecimalVector) getInput(), initialCapacity);
  }

  @Override
  protected BigDecimal getElement(
      long baseAddress, int itemIndex, ArrowBuf dataBuffer, ArrowBuf offsetBuffer) {
    long offHeapMemoryAddress = getOffHeapAddressForFixedWidthTypes(baseAddress, itemIndex);
    final int scale = ((DecimalVector) getInput()).getScale();
    return DecimalAccumulatorUtils.getBigDecimal(offHeapMemoryAddress, valueBuffer, scale);
  }
}

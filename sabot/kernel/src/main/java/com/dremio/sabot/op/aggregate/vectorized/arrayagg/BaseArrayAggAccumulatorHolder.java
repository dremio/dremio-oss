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

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.SmallIntVector;

/**
 * Base class for temporary batches accumulated by {@link BaseArrayAggAccumulator}.
 *
 * <p>This forms an abstraction layer that handles grouping based on chunk offsets. During the
 * output phase, it passes back grouped elements in form of Iterator.
 *
 * @param <ElementType> Type of individual accumulated element
 * @param <VectorType> Type of temporary vector that's used for holding collection of elements
 */
public abstract class BaseArrayAggAccumulatorHolder<ElementType, VectorType extends FieldVector> {
  private int numItems;
  private int maxGroupIdentifier;
  private final SmallIntVector groupIndexVector;
  private final int maxValuesPerBatch;

  public final class ElementsGroup implements Iterator<ElementType> {
    private final int elementsGroupIndex;
    private int current;
    private int next;

    public ElementsGroup(int elementsGroupIndex) {
      this.elementsGroupIndex = elementsGroupIndex;
      current = -1;
      next = -1;
    }

    @Override
    public boolean hasNext() {
      for (next = current + 1; next < numItems; next++) {
        if (groupIndexVector.get(next) == elementsGroupIndex) {
          break;
        }
      }
      current = next;
      return next < numItems;
    }

    @Override
    public ElementType next() {
      return getItem(current);
    }
  }

  protected BaseArrayAggAccumulatorHolder(int maxValuesPerBatch, BufferAllocator allocator) {
    this.numItems = 0;
    this.maxGroupIdentifier = 0;
    this.maxValuesPerBatch = maxValuesPerBatch;
    this.groupIndexVector =
        new SmallIntVector("BaseArrayAggAccumulatorHolder indexVector", allocator);
    this.groupIndexVector.allocateNew(maxValuesPerBatch);
  }

  public void addItem(ElementType data, int chunkOffset) {
    Preconditions.checkArgument(
        numItems < maxValuesPerBatch,
        "One or more of the ARRAY_AGG groups contains too many elements");
    maxGroupIdentifier = Math.max(maxGroupIdentifier, chunkOffset);
    addItemToVector(data, numItems);
    groupIndexVector.set(numItems, chunkOffset);
    numItems++;
  }

  public Iterator<ElementsGroup> getGroupsIterator() {
    return IntStream.range(0, maxGroupIdentifier + 1)
        .mapToObj(x -> new ElementsGroup(x))
        .iterator();
  }

  public long getSizeInBytes() {
    return groupIndexVector.getDataBuffer().getActualMemoryConsumed()
        + groupIndexVector.getValidityBuffer().getActualMemoryConsumed();
  }

  public void close() {
    groupIndexVector.close();
  }

  public abstract void addItemToVector(ElementType data, int index);

  public abstract ElementType getItem(int index);
}

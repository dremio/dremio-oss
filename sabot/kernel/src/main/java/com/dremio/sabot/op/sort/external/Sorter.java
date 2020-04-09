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
package com.dremio.sabot.op.sort.external;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector4;

public interface Sorter extends  AutoCloseable {
  public boolean expandMemoryIfNecessary(int newRequiredSize);
  public void setup(VectorAccessible batch) throws ClassTransformationException, SchemaChangeException, IOException;
  public void addBatch(RecordBatchData data, BufferAllocator copyTargetAllocator) throws SchemaChangeException;
  public ExpandableHyperContainer getHyperBatch();
  public int getHyperBatchSize();
  public SelectionVector4 getFinalSort(BufferAllocator copyTargetAllocator, int targetBatchSize);
  public void close() throws Exception;
}

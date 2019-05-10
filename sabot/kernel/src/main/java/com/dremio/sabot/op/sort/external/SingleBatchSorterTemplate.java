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
package com.dremio.sabot.op.sort.external;

import java.util.concurrent.TimeUnit;

import javax.inject.Named;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

public abstract class SingleBatchSorterTemplate implements SingleBatchSorterInterface, IndexedSortable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleBatchSorterTemplate.class);

  private SelectionVector2 vector2;

  public void setup(FunctionContext context, SelectionVector2 vector2, VectorAccessible incoming) throws SchemaChangeException{
    Preconditions.checkNotNull(vector2);
    this.vector2 = vector2;
    try {
      doSetup(context, incoming, null);
    } catch (IllegalStateException e) {
      throw new SchemaChangeException(e);
    }
  }

  @Override
  public void sort(SelectionVector2 vector2){
    QuickSort qs = new QuickSort();
    Stopwatch watch = Stopwatch.createStarted();
    if (vector2.getCount() > 0) {
      qs.sort(this, 0, vector2.getCount());
    }
    logger.debug("Took {} us to sort {} records", watch.elapsed(TimeUnit.MICROSECONDS), vector2.getCount());
  }

  @Override
  public void swap(int sv0, int sv1) {
    char tmp = vector2.getIndex(sv0);
    vector2.setIndex(sv0, vector2.getIndex(sv1));
    vector2.setIndex(sv1, tmp);
  }

  @Override
  public int compare(int leftIndex, int rightIndex) {
    char sv1 = vector2.getIndex(leftIndex);
    char sv2 = vector2.getIndex(rightIndex);
    return doEval(sv1, sv2);
  }

  public abstract void doSetup(@Named("context") FunctionContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);
  public abstract int doEval(@Named("leftIndex") char leftIndex, @Named("rightIndex") char rightIndex);

}

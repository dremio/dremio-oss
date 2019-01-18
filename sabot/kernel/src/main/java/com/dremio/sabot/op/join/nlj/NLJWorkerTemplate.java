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
package com.dremio.sabot.op.join.nlj;

import java.util.LinkedList;
import java.util.List;

import javax.inject.Named;

import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;

/*
 * Template class that combined with the runtime generated source implements the NestedLoopJoin interface. This
 * class contains the main nested loop join logic.
 */
public abstract class NLJWorkerTemplate implements NLJWorker {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NLJWorkerTemplate.class);

  // Current left input batch being processed
  private VectorAccessible left = null;

  // List of record counts  per batch in the hyper container
  private List<Integer> rightCounts = null;

  private int leftIndex = 0;
  private int rightBatchNumber = 0;
  private int rightIndex = 0;

  /**
   * Method initializes necessary state and invokes the doSetup() to set the
   * input and output value vector references
   * @param context Fragment context
   * @param left Current left input batch being processed
   * @param rightContainer Hyper container
   * @param outgoing Output batch
   */
  public void setupNestedLoopJoin(
      FunctionContext context,
      VectorAccessible left,
      ExpandableHyperContainer rightContainer,
      LinkedList<Integer> rightCounts,
      VectorAccessible outgoing){
    this.left = left;
    this.rightCounts = rightCounts;

    doSetup(context, rightContainer, left, outgoing);
  }

  /**
   * Emit records for the join.
   * @param outputIndex
   * @param targetTotalOutput
   * @return
   */
  public int emitRecords(int outputIndex, int targetTotalOutput) {
    assert outputIndex < targetTotalOutput;

    // Total number of batches on the right side
    final int totalRightBatches = rightCounts.size();

    // Total number of records on the left
    final int localLeftRecordCount = left.getRecordCount();

     logger.debug("Left record count: {}, Right batch count: {}, OutputIndex: {}, Target output {}", localLeftRecordCount, totalRightBatches, outputIndex, targetTotalOutput);

    /*
     * The below logic is the core of the NLJ. To have better performance we copy the instance members into local
     * method variables, once we are done with the loop we need to update the instance variables to reflect the new
     * state. To avoid code duplication of resetting the instance members at every exit point in the loop we are using
     * an break :outer.
     */
    int lLeftIndex = this.leftIndex;
    int lRightBatchNumber = this.rightBatchNumber;
    int lRightIndex = this.rightIndex;
    boolean finishedCurrentLeft = true;

    logger.debug("[Enter] Left: {}, Right {}:{}.",  leftIndex, lRightBatchNumber, rightIndex);

    outer: {

      for (; lRightBatchNumber < totalRightBatches; lRightBatchNumber++) {
        // for every batch on the right

        final int rightRecordCount = rightCounts.get(lRightBatchNumber);

        for (; lRightIndex < rightRecordCount; lRightIndex++) {
          // for every record in this right batch

          for (; lLeftIndex < localLeftRecordCount; lLeftIndex++) {
            // for every record in the left batch

            logger.trace("Left: {}, Right: {}:{}, Output: {}", lLeftIndex, lRightBatchNumber, lRightIndex, outputIndex);

            // project records from the left and right batches
            emitLeft(lLeftIndex, outputIndex);
            emitRight(lRightBatchNumber, lRightIndex, outputIndex);
            outputIndex++;

            // TODO: Optimization; We can eliminate this check and compute the
            // limits before the loop
            if (outputIndex == targetTotalOutput) {
              lLeftIndex++;

              // no more space left in the batch, stop processing
              finishedCurrentLeft = false;
              break outer;
            }
          }
          lLeftIndex = 0;
        }
        lRightIndex = 0;
      }
    }

    if(finishedCurrentLeft){
      this.rightBatchNumber = 0;
      this.rightIndex = 0;
      this.leftIndex = 0;
      logger.debug("[Exit] Left: {}, Right {}:{}, OutputIndex: {}",  leftIndex, rightBatchNumber, rightIndex, outputIndex);
      return outputIndex;
    } else {
      // save offsets
      this.rightBatchNumber = lRightBatchNumber;
      this.rightIndex = lRightIndex;
      this.leftIndex = lLeftIndex;
       logger.debug("[Exit] Left: {}, Right {}:{}, OutputIndex: {}",  leftIndex, rightBatchNumber, rightIndex, outputIndex);
      return -outputIndex;
    }
  }


  public abstract void doSetup(@Named("context") FunctionContext context,
                               @Named("rightContainer") VectorAccessible rightContainer,
                               @Named("leftBatch") VectorAccessible leftBatch,
                               @Named("outgoing") VectorAccessible outgoing);

  public abstract void emitRight(@Named("batchIndex") int batchIndex,
                                 @Named("recordIndexWithinBatch") int recordIndexWithinBatch,
                                 @Named("outIndex") int outIndex);

  public abstract void emitLeft(@Named("leftIndex") int leftIndex, @Named("outIndex") int outIndex);
}

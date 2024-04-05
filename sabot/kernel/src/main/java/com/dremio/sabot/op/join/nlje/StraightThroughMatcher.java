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
package com.dremio.sabot.op.join.nlje;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import java.util.List;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A matcher that just returns the probe side of the dataset. This is used when we're doing a LEFT
 * join and there were no build batches.
 */
class StraightThroughMatcher implements JoinMatcher {

  private long totalRecords;
  private int records;
  private final List<TransferPair> probeTransfers;
  private final VectorContainer output;

  public StraightThroughMatcher(VectorContainer output, List<TransferPair> probeTransfers) {
    this.probeTransfers = probeTransfers;
    this.output = output;
  }

  @Override
  public int output() {
    if (records > 0) {
      for (TransferPair p : probeTransfers) {
        p.transfer();
      }
      output.setAllCount(records);
      totalRecords += records;
      int local = records;
      this.records = 0;
      return local;
    }

    output.setAllCount(0);
    return 0;
  }

  @Override
  public long getCopyNanos() {
    return 0;
  }

  @Override
  public long getMatchNanos() {
    return 0;
  }

  @Override
  public long getProbeCount() {
    return totalRecords;
  }

  @Override
  public void setup(
      LogicalExpression expr,
      ClassProducer classProducer,
      VectorAccessible probe,
      VectorAccessible build)
      throws Exception {}

  @Override
  public void startNextProbe(int records) {
    this.records = records;
  }

  @Override
  public boolean needNextInput() {
    return records == 0;
  }

  @Override
  public void close() {}
}

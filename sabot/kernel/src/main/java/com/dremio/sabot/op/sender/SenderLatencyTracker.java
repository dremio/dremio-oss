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
package com.dremio.sabot.op.sender;

import com.dremio.sabot.exec.rpc.SenderLatencyObserver;
import java.util.concurrent.atomic.AtomicLong;

public class SenderLatencyTracker {
  private final AtomicLong sumAckMillis = new AtomicLong();
  private final AtomicLong maxAckMillis = new AtomicLong();

  public SenderLatencyTracker() {}

  public long getMaxAckMillis() {
    return maxAckMillis.get();
  }

  public long getSumAckMillis() {
    return sumAckMillis.get();
  }

  public SenderLatencyObserver getLatencyObserver() {
    return this::updateAckMillis;
  }

  private void updateAckMillis(long ackMillis) {
    sumAckMillis.getAndAdd(ackMillis);
    if (maxAckMillis.get() < ackMillis) {
      // not thread-safe, but we don't need it to be very accurate.
      maxAckMillis.set(ackMillis);
    }
  }
}

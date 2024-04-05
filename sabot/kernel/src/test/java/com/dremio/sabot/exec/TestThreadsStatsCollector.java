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
package com.dremio.sabot.exec;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class TestThreadsStatsCollector {

  @Test
  public void testOldThreadsArePruned() throws InterruptedException {

    Thread t =
        new Thread() {
          @Override
          public void run() {
            try {
              sleep(400L);
            } catch (InterruptedException e) {
            }
          }
        };

    Thread t1 =
        new Thread() {
          @Override
          public void run() {
            try {
              sleep(400L);
            } catch (InterruptedException e) {
            }
          }
        };

    t.start();
    t1.start();

    // it should collect stats only for t1
    ThreadsStatsCollector collector = new ThreadsStatsCollector(50L, Sets.newHashSet(t1.getId()));
    collector.start();

    // allow the collector to get stats
    sleep(200L);
    Integer stat = collector.getCpuTrailingAverage(t.getId(), 1);
    Integer statThread2 = collector.getCpuTrailingAverage(t1.getId(), 1);
    // We should get stats only for thread t1
    Assert.assertTrue(stat == null);
    Assert.assertTrue(statThread2 != null);
    // wait for other threads to exit.
    t.join();
    t1.join();

    // Double check that it is still not present.
    stat = collector.getCpuTrailingAverage(t.getId(), 1);
    Assert.assertTrue(stat == null);
  }

  private void sleep(long l) {
    try {
      Thread.sleep(l);
    } catch (InterruptedException e) {
    }
  }
}

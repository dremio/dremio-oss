/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.carrotsearch.hppc.LongObjectHashMap;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.AbstractMap.SimpleEntry;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Collects stats to report CPU usage per thread during the last 5 seconds
 */
public class ThreadsStatsCollector extends Thread implements AutoCloseable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ThreadsStatsCollector.class);

  private static final long ONE_BILLION = 1000000000;
  private static final long RETAIN_INTERVAL = 5 * ONE_BILLION; // in nanoseconds
  private static final int COLLECTION_INTERVAL = 1; // in seconds

  private ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
  private ThreadStat cpuStat = new ThreadStat();
  private ThreadStat userStat = new ThreadStat();


  public ThreadsStatsCollector() {
    super("thread-stats-collector");
  }

  @Override
  public void run() {
    Thread.currentThread().setName("thread-stats-collector");
    while (true) {
      try {
        Thread.sleep(COLLECTION_INTERVAL * 1000);
        addCpuTime();
        addUserTime();
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  public Integer getCpuTrailingAverage(long id, int seconds) {
    return cpuStat.getTrailingAverage(id, seconds);
  }

  public Integer getUserTrailingAverage(long id, int seconds) {
    return userStat.getTrailingAverage(id, seconds);
  }

  private void addCpuTime() {
    for (long id : mxBean.getAllThreadIds()) {
      cpuStat.add(id, System.nanoTime(), mxBean.getThreadCpuTime(id));
    }
  }

  private void addUserTime() {
    for (long id : mxBean.getAllThreadIds()) {
      userStat.add(id, System.nanoTime(), mxBean.getThreadUserTime(id));
    }
  }

  private static class ThreadStat {
    volatile LongObjectHashMap<Deque<Entry<Long,Long>>> data = new LongObjectHashMap<>();

    public void add(long id, long ts, long value) {
      Entry<Long,Long> entry = new SimpleEntry<>(ts, value);
      Deque<Entry<Long,Long>> list = data.get(id);
      if (list == null) {
        list = new ConcurrentLinkedDeque<>();
      }
      list.add(entry);
      while (ts - list.peekFirst().getKey() > RETAIN_INTERVAL) {
        list.removeFirst();
      }
      data.put(id, list);
    }

    public Integer getTrailingAverage(long id, int seconds) {
      Deque<Entry<Long,Long>> list = data.get(id);
      if (list == null) {
        return null;
      }
      return getTrailingAverage(list, seconds);
    }

    private Integer getTrailingAverage(Deque<Entry<Long, Long>> list, int seconds) {
      Entry<Long,Long> latest = list.peekLast();
      Entry<Long,Long> old = list.peekFirst();
      Iterator<Entry<Long,Long>> iter = list.descendingIterator();
      while (iter.hasNext()) {
        Entry<Long,Long> e = iter.next();
        if (e.getKey() - latest.getKey() > seconds * ONE_BILLION) {
          old = e;
          break;
        }
      }

      final long oldKey = old.getKey();
      final long latestKey = latest.getKey();
      if (oldKey == latestKey) {
        return null;
      } else {
        return (int) (100 * (old.getValue() - latest.getValue()) / (oldKey - latestKey));
      }
    }
  }

  public void close(){
    this.interrupt();
  }
}

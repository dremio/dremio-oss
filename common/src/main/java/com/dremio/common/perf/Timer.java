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
package com.dremio.common.perf;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility to trace time spent in services
 */
public final class Timer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Timer.class);

  private static ThreadLocal<Ticker> tickerPerThread = new ThreadLocal<>();

  public static Ticker longLivedTicker() {
    Ticker ticker = new Ticker();
    tickerPerThread.set(ticker);
    return ticker;
  }

  public static void release() {
    tickerPerThread.remove();
  }

  public static boolean enabled() {
    return logger.isTraceEnabled();
  }

  private Timer() {
  }

  private static long nextAsyncId = 0;

  public static TimedBlock time(final String name) {
    Ticker currentTicker = tickerPerThread.get();
    if (currentTicker != null) {
      return new TimedBlock(name, currentTicker);
    } else {
      final Ticker newTicker = longLivedTicker();
      return new TimedBlock(name, newTicker) {
        @Override
        public void close() {
          super.close();
          newTicker.log(nextAsyncId++, "async " + name);
          release();
        }
      };
    }
  }

  /**
   * To be used in 'try with resource' blocks to track time
   */
  public static class TimedBlock implements AutoCloseable {
    private final int id;
    private final String name;
    private final Ticker ticker;

    public TimedBlock(String name, Ticker ticker) {
      this.ticker = ticker;
      this.name = name;
      this.id = ticker.nextId();
      ticker.tick(name, id);
    }

    public void addID(String id) {
      ticker.addID(id);
    }

    @Override
    public void close() {
      ticker.tock(name, id);
    }
  }

  /**
   * a trace event
   */
  private static final class Event {
    private final long t;
    private final String name;
    private final boolean tick;
    private final int id;

    public Event(String name, long t, boolean tick, int id) {
      this.name = name;
      this.t = t;
      this.tick = tick;
      this.id = id;
    }
  }

  /**
   * to track trace events
   */
  public static final class Ticker {
    private final List<Event> events = new ArrayList<>();

    private int nextId = 0;
    private final long start;
    private Set<String> customIDs;

    private Ticker() {
      start = System.nanoTime();
    }

    public void addID(String id) {
      if (customIDs == null) {
        customIDs = new LinkedHashSet<>(1);
      }
      customIDs.add(id);
    }

    public int nextId() {
      return nextId++;
    }

    public void tick(String name, int id) {
      event(name, true, id);
    }

    public void tock(String name, int id) {
      event(name, false, id);
    }

    private void event(String name, boolean tick, int id) {
      long t = System.nanoTime();
      events.add(new Event(name, (t - start) / 1000000, tick, id));
    }

    public void log(long reqId, String message) {
      if (!logger.isTraceEnabled()) {
        return;
      }

      StringBuilder sb = new StringBuilder();
      sb.append(reqId).append(": ");
      sb.append((System.nanoTime() - start)/1000000).append("ms ");
      sb.append(message);
      if (customIDs != null) {
        for (String customID : customIDs) {
          sb.append(" ").append(customID);
        }
      }
      int indent = 15;
      Event previous = null;
      for (Event e : events) {
        String ts = (previous == null ? e.t : (e.t - previous.t)) + "ms";
        String pr = reqId + "-" + e.id + ": " + ts;
        if (e.tick) {
          sb.append("\n");
          sb.append(pr);
          for (int i = pr.length(); i < indent; ++i) {
            sb.append(" ");
          }
          sb.append("{ ");
          sb.append(e.name);
          ++ indent;
        } else if (
            previous != null
            && previous.tick
            && previous.name.equals(e.name)
            && previous.id == e.id) {
          -- indent;
          sb.append(" ").append(ts).append(" }");
        } else {
          -- indent;
          sb.append("\n");
          sb.append(pr);
          for (int i = pr.length(); i < indent; ++i) {
            sb.append(" ");
          }
          sb.append("} ");
          sb.append(e.name);
        }
        previous = e;
      }
      logger.trace(sb.toString());
    }
  }
}

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
package com.dremio.service.scheduler;

import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class TestStaggeredSchedule {
  @Test
  public void testStaggeredSchedule() {
    Instant startInstant = Instant.now().plusSeconds(3600);
    Schedule baseSchedule =
        Schedule.Builder.everyDays(1, LocalTime.MIDNIGHT)
            .withTimeZone(ZoneId.of("UTC-8"))
            .startingAt(startInstant)
            .build();

    Schedule staggeredSchedule =
        Schedule.Builder.everyDays(1, LocalTime.MIDNIGHT)
            .withTimeZone(ZoneId.of("UTC-8"))
            .startingAt(startInstant)
            .staggered(10, 1L, TimeUnit.HOURS)
            .build();

    Iterator<Instant> baseIter = baseSchedule.iterator();
    Iterator<Instant> staggeredIter = staggeredSchedule.iterator();
    int numEquals = 0;
    int numAfter = 0;
    int numBefore = 0;
    for (int i = 0; i < 1000; i++) {
      Instant base = baseIter.next();
      Instant staggered = staggeredIter.next();
      long diff;
      if (staggered.equals(base)) {
        numEquals++;
        diff = 0;
      } else if (staggered.isAfter(base)) {
        numAfter++;
        diff = staggered.getEpochSecond() - base.getEpochSecond();
      } else {
        numBefore++;
        diff = base.getEpochSecond() - staggered.getEpochSecond();
      }

      assertTrue("diff should be less than one hour", (diff <= 3600));
    }

    assertTrue("numAfter should be > 0", numAfter > 0);
    assertTrue("numBefore should be > 0", numBefore > 0);
  }

  @Test
  public void testHalfPeriodStagger() {
    Instant startInstant = Instant.now().plusSeconds(3600);

    Schedule baseSchedule =
        Schedule.Builder.everyHours(1)
            .withTimeZone(ZoneId.of("UTC-8"))
            .startingAt(startInstant)
            .build();

    Schedule staggeredSchedule =
        Schedule.Builder.everyHours(1)
            .withTimeZone(ZoneId.of("UTC-8"))
            .startingAt(startInstant)
            .staggered(10, 2L, TimeUnit.HOURS)
            .build();

    Iterator<Instant> baseIter = baseSchedule.iterator();
    Iterator<Instant> staggeredIter = staggeredSchedule.iterator();
    for (int i = 0; i < 1000; i++) {
      Instant base = baseIter.next();
      Instant staggered = staggeredIter.next();
      long diff;
      if (staggered.equals(base)) {
        diff = 0;
      } else if (staggered.isAfter(base)) {
        diff = staggered.getEpochSecond() - base.getEpochSecond();
      } else {
        diff = base.getEpochSecond() - staggered.getEpochSecond();
      }

      assertTrue("diff should be less than 30 mins", (diff <= 1800));
    }
  }
}

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
package com.dremio.service.scheduler;

import static org.junit.Assert.assertEquals;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;

/**
 * Unit test for {@link Schedule}
 */
@RunWith(Parameterized.class)
public class TestSchedule {
  private final Schedule schedule;
  private final List<Instant> expected;

  private static Object[] newTestCase(String name, Schedule schedule, String[] events) {
    ImmutableList.Builder<Instant> builder = ImmutableList.builder();
    for(String event: events) {
      builder.add(Instant.parse(event));
    }

    return new Object[] { name, schedule, builder.build() };
  }

  @Parameters(name = "{index}: {0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        newTestCase(
            "hourly schedule",
            Schedule.Builder.everyHours(1).startingAt(Instant.parse("2016-03-24T21:13:47Z")).build(),
            new String[] { "2016-03-24T21:13:47Z", "2016-03-24T22:13:47Z", "2016-03-24T23:13:47Z", "2016-03-25T00:13:47Z" }),
        newTestCase(
            "every 2 hours schedule",
            Schedule.Builder.everyHours(2).startingAt(Instant.parse("2016-03-24T21:15:24Z")).build(),
            new String[] { "2016-03-24T21:15:24Z", "2016-03-24T23:15:24Z", "2016-03-25T01:15:24Z", "2016-03-25T03:15:24Z" }),
        newTestCase(
            "daily schedule",
            Schedule.Builder.everyDays(1).startingAt(Instant.parse("2016-02-28T21:15:24Z")).build(),
            new String[] { "2016-02-28T21:15:24Z", "2016-02-29T21:15:24Z", "2016-03-01T21:15:24Z", "2016-03-02T21:15:24Z" }),
        newTestCase(
            "every 3 days at 18:26:54",
            Schedule.Builder.everyDays(3, LocalTime.parse("18:26:54")).startingAt(Instant.parse("2016-02-26T21:15:24Z")).build(),
            new String[] { "2016-02-27T18:26:54Z", "2016-03-01T18:26:54Z", "2016-03-04T18:26:54Z", "2016-03-07T18:26:54Z" }),
        newTestCase(
            "bi-weekly schedule",
            Schedule.Builder.everyWeeks(2).startingAt(Instant.parse("2016-04-17T01:42:34Z")).build(),
            new String[] { "2016-04-17T01:42:34Z", "2016-05-01T01:42:34Z", "2016-05-15T01:42:34Z", "2016-05-29T01:42:34Z" }),
        newTestCase(
            "every other tuesday schedule",
            Schedule.Builder.everyWeeks(2, DayOfWeek.TUESDAY, LocalTime.parse("19:31:17")).startingAt(Instant.parse("2016-05-11T22:34:54Z")).build(),
            new String[] { "2016-05-17T19:31:17Z", "2016-05-31T19:31:17Z", "2016-06-14T19:31:17Z", "2016-06-28T19:31:17Z" }),
        newTestCase(
            "last day of month",
            Schedule.Builder.everyMonths(1, 31, LocalTime.parse("13:17:00")).startingAt(Instant.parse("2016-01-01T11:12:13Z")).build(),
            new String[] { "2016-01-31T13:17:00Z", "2016-02-29T13:17:00Z", "2016-03-31T13:17:00Z", "2016-04-30T13:17:00Z" }),
        newTestCase(
            "daily schedule in a specified time zone",
            Schedule.Builder.everyDays(1, LocalTime.MIDNIGHT).withTimeZone(ZoneId.of("UTC-8")).startingAt(Instant.parse("2016-01-01T11:12:13Z")).build(),
            new String[] { "2016-01-02T08:00:00Z", "2016-01-03T08:00:00Z", "2016-01-04T08:00:00Z", "2016-01-05T08:00:00Z" })
        );
  }
  public TestSchedule(String name, Schedule schedule, List<Instant> expected) {
    this.schedule = schedule;
    this.expected = expected;
  }

  @Test
  public void testSchedule() {
    Iterator<Instant> instants = schedule.iterator();
    for(Instant expectedInstant: expected) {
      assertEquals(expectedInstant, instants.next());
    }
  }
}

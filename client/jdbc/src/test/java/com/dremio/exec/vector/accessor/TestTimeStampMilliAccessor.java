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
package com.dremio.exec.vector.accessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.junit.Before;
import org.junit.Test;

public class TestTimeStampMilliAccessor {

  public static final long NON_NULL_VALUE = 89723408957L; // 1972-11-04 11:10:08.957
  public static final long DST_VALUE = 1558999993123L; // 2019-05-27 23:33:13.123
  private static final Calendar PST_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("PST"));
  private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  private SqlAccessor accessor;
  private TimeStampMilliVector valueVector;

  @Before
  public void setUp() {
    valueVector = new TimeStampMilliVector("t", new RootAllocator());
    valueVector.allocateNew(3);
    valueVector.set(0, NON_NULL_VALUE);
    valueVector.set(1, DST_VALUE);
    valueVector.setNull(2);

    accessor = new TimeStampMilliAccessor(valueVector, UTC_CALENDAR.getTimeZone());
  }

  @Test
  public void testIsNull() throws Exception {
    assertNotNull(accessor.getObject(0));
    assertNotNull(accessor.getTimestamp(0, PST_CALENDAR));
    assertNull(accessor.getObject(2));
    assertNull(accessor.getTimestamp(2, PST_CALENDAR));
  }

  @Test
  public void testGetObject() throws Exception {
    assertEquals(
      new Timestamp(72, 10, 4, 11, 10, 8, (int)TimeUnit.MILLISECONDS.toNanos(957)),
      accessor.getObject(0));
  }

  @Test(expected=NullPointerException.class)
  public void testNullCalendar() throws InvalidAccessException {
    accessor.getTimestamp(0, null);
  }

  @Test(expected=NullPointerException.class)
  public void testCreationWithNullTimeZone() {
    new TimeStampMilliAccessor(valueVector, null);
  }

  @Test
  public void testGetTimestamp() throws Exception {
    assertEquals(
      new Timestamp(72, 10, 4, 3, 10, 8, (int)TimeUnit.MILLISECONDS.toNanos(957)),
      accessor.getTimestamp(0, PST_CALENDAR));
    assertEquals(
      new Timestamp(72, 10, 4, 11, 10, 8, (int)TimeUnit.MILLISECONDS.toNanos(957)),
      accessor.getTimestamp(0, UTC_CALENDAR));

    assertEquals(
      new Timestamp(119, 4, 27, 16, 33, 13, (int)TimeUnit.MILLISECONDS.toNanos(123)),
      accessor.getTimestamp(1, PST_CALENDAR));
    assertEquals(
      new Timestamp(119, 4, 27, 23, 33, 13, (int)TimeUnit.MILLISECONDS.toNanos(123)),
      accessor.getTimestamp(1, UTC_CALENDAR));
  }
}

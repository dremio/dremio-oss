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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateMilliVector;
import org.junit.Before;
import org.junit.Test;

public class TestDateMilliAccessor {

  public static final long NON_NULL_VALUE = 89683200000L; // 1972-11-04
  public static final long DST_VALUE = 1558999993123L; // 2019-05-27 16:33:13.123
  private static final Calendar PST_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("PST"));
  private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  private SqlAccessor accessor;
  private DateMilliVector valueVector;

  @Before
  public void setUp() {
    valueVector = new DateMilliVector("t", new RootAllocator());
    valueVector.allocateNew(3);
    valueVector.set(0, NON_NULL_VALUE);
    valueVector.set(1, DST_VALUE);
    valueVector.setNull(2);

    accessor = new DateMilliAccessor(valueVector, UTC_CALENDAR.getTimeZone());
  }

  @Test
  public void testIsNull() throws Exception {
    assertNotNull(accessor.getObject(0));
    assertNotNull(accessor.getDate(0, PST_CALENDAR));
    assertNull(accessor.getObject(2));
    assertNull(accessor.getDate(2, PST_CALENDAR));
  }

  @Test
  public void testGetObject() throws Exception {
    assertEquals(new Date(72, 10, 4), accessor.getObject(0));
  }

  @Test(expected=NullPointerException.class)
  public void testNullCalendar() throws InvalidAccessException {
    accessor.getDate(0, null);
  }

  @Test(expected=NullPointerException.class)
  public void testCreationWithNullTimeZone() {
    new DateMilliAccessor(valueVector, null);
  }

  @Test
  public void testGetDate() throws Exception {
    assertEquals(new Date(72, 10, 3), accessor.getDate(0, PST_CALENDAR));
    assertEquals(new Date(72, 10, 4), accessor.getDate(0, UTC_CALENDAR));

    assertEquals(new Date(119, 4, 27), accessor.getDate(1, PST_CALENDAR));
    assertEquals(new Date(119, 4, 27), accessor.getDate(1, UTC_CALENDAR));
  }
}

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
package com.dremio.services.nodemetrics.persistence;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecondPrecisionDateTimeFormatter {
  private static final SecondPrecisionDateTimeFormatter FORMATTER =
      new SecondPrecisionDateTimeFormatter();

  @Test
  public void testFormatDateTime() {
    Instant instant = Instant.ofEpochMilli(1000);
    String expected = "19700101T000001Z";
    Assertions.assertEquals(expected, FORMATTER.formatDateTime(instant));
  }

  @Test
  public void testGetDateTimeGlob() {
    String expected = "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z";
    Assertions.assertEquals(expected, FORMATTER.getDateTimeGlob());
  }

  @Test
  public void testParseDateTime() {
    String dateTime = "19700101T000001Z";
    ZonedDateTime expected = ZonedDateTime.ofInstant(Instant.ofEpochMilli(1000), ZoneId.of("UTC"));
    Assertions.assertEquals(expected, FORMATTER.parseDateTime(dateTime));
  }

  @Test
  public void testFormatAndParse() {
    Instant now = Instant.now();
    Instant expected = now.truncatedTo(ChronoUnit.SECONDS);
    Assertions.assertEquals(
        expected, FORMATTER.parseDateTime(FORMATTER.formatDateTime(now)).toInstant());
  }
}

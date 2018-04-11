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
package com.dremio.exec.vector.accessor.sql;

import java.sql.Time;

import org.joda.time.LocalDateTime;

public class TimePrintMillis extends Time {
  private static final String[] leadingZeroes = {"", "0", "00"};

  // Desired length of the milli second portion should be 3
  private static final int DESIRED_MILLIS_LENGTH = 3;

  // Millis of the date time object.
  final private int millisOfSecond;

  public TimePrintMillis(LocalDateTime time) {
    super(time.getHourOfDay(), time.getMinuteOfHour(), time.getSecondOfMinute());
    millisOfSecond = time.getMillisOfSecond();
  }

  @Override
  public String toString () {
    StringBuilder time = new StringBuilder().append(super.toString());

    if (millisOfSecond > 0) {
      String millisString = Integer.toString(millisOfSecond);

      // dot to separate the fractional seconds
      time.append(".");

      int millisLength = millisString.length();
      if (millisLength < DESIRED_MILLIS_LENGTH) {
        // add necessary leading zeroes
        time.append(leadingZeroes[DESIRED_MILLIS_LENGTH - millisLength]);
      }
      time.append(millisString);
    }

    return time.toString();
  }
}


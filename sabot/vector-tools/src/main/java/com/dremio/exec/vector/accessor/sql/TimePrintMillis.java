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
package com.dremio.exec.vector.accessor.sql;

import java.sql.Time;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Objects;

import com.dremio.common.SuppressForbidden;
import com.google.common.collect.ImmutableList;

public class TimePrintMillis extends Time {
  private static final List<String> LEADING_ZEROES = ImmutableList.of("", "0", "00");

  // Desired length of the milli second portion should be 3
  private static final int DESIRED_MILLIS_LENGTH = 3;

  // Millis of the date time object.
  private final int millisOfSecond;

  @SuppressForbidden
  public TimePrintMillis(LocalTime time) {
    // Although the constructor is deprecated, this is the exact same code as Time#valueOf(LocalTime)
    super(time.getHour(), time.getMinute(), time.getSecond());
    millisOfSecond = time.get(ChronoField.MILLI_OF_SECOND);
  }

  @Override
  public boolean equals(Object obj) {
    if ((obj instanceof TimePrintMillis) && super.equals(obj)) {
      return this.millisOfSecond == ((TimePrintMillis)obj).millisOfSecond;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), this.millisOfSecond);
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
        time.append(LEADING_ZEROES.get(DESIRED_MILLIS_LENGTH - millisLength));
      }
      time.append(millisString);
    }

    return time.toString();
  }
}

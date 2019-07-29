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

package com.dremio.exec.util;

import java.util.List;

import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;

import com.dremio.common.exceptions.UserException;
import com.google.common.collect.ImmutableList;

public enum TSI {
  MICROSECOND(ImmutableList.<String>builder()
      .add("FRAC_SECOND")
      .add("MICROSECOND")
      .add("SQL_TSI_FRAC_SECOND")
      .add("SQL_TSI_MICROSECOND")
      .build()) {
    @Override
    public void addCount(MutableDateTime dateTime, int count) {
      // TODO (DX-11268): Fix TIMESTAMPADD(SQL_TSI_FRAC_SECOND, ..., ...) function
      throw new UnsupportedOperationException("Fractional second is not supported");
    }

    @Override
    public long getDiff(Interval interval) {
      // TODO (DX-11268): Fix TIMESTAMPADD(SQL_TSI_FRAC_SECOND, ..., ...) function
      throw new UnsupportedOperationException("Fractional second is not supported");
    }
  },
  SECOND(ImmutableList.<String>builder().add("SECOND").add("SQL_TSI_SECOND").build()) {
    @Override
    public void addCount(MutableDateTime dateTime, int count) {
      dateTime.addSeconds(count);
    }

    @Override
    public long getDiff(Interval interval) {
      return Seconds.secondsIn(interval).getSeconds();
    }
  },
  MINUTE(ImmutableList.<String>builder().add("MINUTE").add("SQL_TSI_MINUTE").build()) {
    @Override
    public void addCount(MutableDateTime dateTime, int count) {
      dateTime.addMinutes(count);
    }

    @Override
    public long getDiff(Interval interval) {
      return Minutes.minutesIn(interval).getMinutes();
    }
  },
  HOUR(ImmutableList.<String>builder().add("HOUR").add("SQL_TSI_HOUR").build()) {
    @Override
    public void addCount(MutableDateTime dateTime, int count) {
      dateTime.addHours(count);
    }

    @Override
    public long getDiff(Interval interval) {
      return Hours.hoursIn(interval).getHours();
    }
  },
  DAY(ImmutableList.<String>builder().add("DAY").add("SQL_TSI_DAY").build()) {
    @Override
    public void addCount(MutableDateTime dateTime, int count) {
      dateTime.addDays(count);
    }

    @Override
    public long getDiff(Interval interval) {
      return Days.daysIn(interval).getDays();
    }
  },
  WEEK(ImmutableList.<String>builder().add("WEEK").add("SQL_TSI_WEEK").build()) {
    @Override
    public void addCount(MutableDateTime dateTime, int count) {
      dateTime.addWeeks(count);
    }

    @Override
    public long getDiff(Interval interval) {
      return Weeks.weeksIn(interval).getWeeks();
    }
  },
  MONTH(ImmutableList.<String>builder().add("MONTH").add("SQL_TSI_MONTH").build()) {
    @Override
    public void addCount(MutableDateTime dateTime, int count) {
      dateTime.addMonths(count);
    }

    @Override
    public long getDiff(Interval interval) {
      return Months.monthsIn(interval).getMonths();
    }
  },
  QUARTER(ImmutableList.<String>builder().add("QUARTER").add("SQL_TSI_QUARTER").build()) {
    @Override
    public void addCount(MutableDateTime dateTime, int count) {
      dateTime.addMonths(3 * count);
    }

    @Override
    public long getDiff(Interval interval) {
      return Months.monthsIn(interval).getMonths() / 3;
    }
  },
  YEAR(ImmutableList.<String>builder().add("YEAR").add("SQL_TSI_YEAR").build()) {
    @Override
    public void addCount(MutableDateTime dateTime, int count) {
      dateTime.addYears(count);
    }

    @Override
    public long getDiff(Interval interval) {
      return Years.yearsIn(interval).getYears();
    }
  };

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TSI.class);

  private List<String> names;

  TSI(List<String> names) {
    this.names = names;
  }

  public static TSI getByName(String name) {
    for (TSI interval : TSI.values()) {
      if (interval.names.contains(name)) {
        return interval;
      }
    }
    return null;
  }

  public static List<String> getAllNames() {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (TSI interval : TSI.values()) {
      builder.addAll(interval.names);
    }
    return builder.build();
  }

  public List<String> getNames() {
    return names;
  }

  public abstract void addCount(MutableDateTime dateTime, int count);

  public void addCount(MutableDateTime dateTime, long count) {
    if (count > Integer.MAX_VALUE || count < Integer.MIN_VALUE) {
      throw UserException.unsupportedError()
        .message("Do not support adding " + count + " " + this.getNames() + " to dateTime. [" + count + "] too large/small for integer.")
        .build(logger);
    }
    addCount(dateTime, (int) count);
  }

  public abstract long getDiff(Interval interval);

}

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
package com.dremio.exec.store.sys;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

/**
 * Class to serve sys.timezone_names table
 */
public class TimezoneNames {

  public static Iterator<Object> getIterator() {
    return Iterators.transform(ZoneId.getAvailableZoneIds().stream().sorted().iterator(), zoneIdToTimezone);
  }

  private static final Function<String, TimezoneRegion> zoneIdToTimezone = (zone) -> {
    Preconditions.checkNotNull(zone);

    Instant now = Instant.now();
    ZoneRules zoneRules = ZoneId.of(zone).getRules();
    ZoneOffset standardOffset = zoneRules.getStandardOffset(now);
    ZoneOffset dsOffset = ZoneOffset.ofTotalSeconds((int) zoneRules.getDaylightSavings(now)
      .plusSeconds(standardOffset.getTotalSeconds())
      .getSeconds());

    String standardOffsetId = standardOffset.getId();
    if (standardOffsetId.equals("Z")) {
      standardOffsetId = "+00:00";
    }

    String dsOffsetId = dsOffset.getId();
    if (dsOffsetId.equals("Z")) {
      dsOffsetId = "+00:00";
    }
    return new TimezoneRegion(zone, standardOffsetId, dsOffsetId, zoneRules.isDaylightSavings(now));
  };

  /**
   * This is the schema for sys.timezone_names
   */
  public static class TimezoneRegion {
    public String timezone_name;
    public String tz_offset;
    public String offset_daylight_savings;
    public boolean is_daylight_savings;

    public TimezoneRegion(String timezone_name, String tz_offset, String offset_daylight_savings, boolean is_daylight_savings) {
      this.timezone_name = timezone_name;
      this.tz_offset = tz_offset;
      this.offset_daylight_savings = offset_daylight_savings;
      this.is_daylight_savings = is_daylight_savings;
    }
  }
}
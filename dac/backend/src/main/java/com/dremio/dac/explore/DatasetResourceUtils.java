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
package com.dremio.dac.explore;

import static com.dremio.service.reflection.DependencyManager.convertDaysOfWeek;
import static com.dremio.service.reflection.DependencyManager.getDayToIntValue;

import com.dremio.catalog.model.VersionContext;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.proto.model.dataset.SourceVersionReference;
import com.dremio.dac.proto.model.dataset.VersionContextType;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Utility classes for DatasetResource */
public final class DatasetResourceUtils {

  private DatasetResourceUtils() {
    // utils class
  }

  public static Map<String, VersionContext> createSourceVersionMapping(
      final Map<String, VersionContextReq> references) {

    final Map<String, VersionContext> sourceVersionMapping = new HashMap<>();
    if (references != null) {
      for (Map.Entry<String, VersionContextReq> entry : references.entrySet()) {
        VersionContextReq.VersionContextType versionContextType = entry.getValue().getType();
        switch (versionContextType) {
          case BRANCH:
            sourceVersionMapping.put(
                entry.getKey(), VersionContext.ofBranch(entry.getValue().getValue()));
            break;
          case TAG:
            sourceVersionMapping.put(
                entry.getKey(), VersionContext.ofTag(entry.getValue().getValue()));
            break;
          case COMMIT:
            sourceVersionMapping.put(
                entry.getKey(), VersionContext.ofCommit(entry.getValue().getValue()));
            break;
          default:
            throw new IllegalArgumentException(
                "Unrecognized versionContextType: " + versionContextType);
        }
      }
    }

    return ImmutableMap.copyOf(sourceVersionMapping);
  }

  public static List<SourceVersionReference> createSourceVersionReferenceList(
      Map<String, VersionContextReq> references) {
    List<SourceVersionReference> sourceVersionReferenceList = new ArrayList<>();
    if (references != null) {
      for (Map.Entry<String, VersionContextReq> entry : references.entrySet()) {
        VersionContextReq versionContextReq = entry.getValue();
        VersionContextReq.VersionContextType versionContextType = versionContextReq.getType();
        String sourceName = entry.getKey();
        switch (versionContextType) {
          case BRANCH:
            com.dremio.dac.proto.model.dataset.VersionContext versionContextBranch =
                new com.dremio.dac.proto.model.dataset.VersionContext(
                    VersionContextType.BRANCH, versionContextReq.getValue());
            sourceVersionReferenceList.add(
                new SourceVersionReference(sourceName, versionContextBranch));
            break;
          case TAG:
            com.dremio.dac.proto.model.dataset.VersionContext versionContextTag =
                new com.dremio.dac.proto.model.dataset.VersionContext(
                    VersionContextType.TAG, versionContextReq.getValue());
            sourceVersionReferenceList.add(
                new SourceVersionReference(sourceName, versionContextTag));
            break;
          case COMMIT:
            com.dremio.dac.proto.model.dataset.VersionContext versionContextCommit =
                new com.dremio.dac.proto.model.dataset.VersionContext(
                    VersionContextType.COMMIT, versionContextReq.getValue());
            sourceVersionReferenceList.add(
                new SourceVersionReference(sourceName, versionContextCommit));
            break;
          default:
            throw new IllegalArgumentException(
                "Unrecognized versionContextType: " + versionContextType);
        }
      }
    }

    return sourceVersionReferenceList;
  }

  public static Map<String, VersionContextReq> createSourceVersionMapping(
      String sourceName, String refType, String refValue) {

    VersionContextReq versionContextReq = VersionContextReq.tryParse(refType, refValue);

    if (versionContextReq != null) {
      return ImmutableMap.<String, VersionContextReq>builder()
          .put(sourceName, versionContextReq)
          .build();
    } else {
      return ImmutableMap.of();
    }
  }

  public static long findLongestPeriodBetweenRefreshes(String cronExpression) {
    String[] cronFields = cronExpression.split(" ");
    String cronDaysOfWeekField = cronFields[5];
    List<Integer> scheduleDaysOfWeek = convertDaysOfWeek(cronDaysOfWeekField);
    if (scheduleDaysOfWeek.size() == 1) {
      return TimeUnit.DAYS.toMillis(7);
    }
    int longestGap = 0;
    for (int i = 1; i < scheduleDaysOfWeek.size(); i++) {
      int currentGap = scheduleDaysOfWeek.get(i) - scheduleDaysOfWeek.get(i - 1);
      longestGap = Math.max(longestGap, currentGap);
    }
    // it's possible the longest gap can wrap across a week, so check that too
    longestGap =
        Math.max(
            longestGap,
            7 - scheduleDaysOfWeek.get(scheduleDaysOfWeek.size() - 1) + scheduleDaysOfWeek.get(0));
    return TimeUnit.DAYS.toMillis(longestGap);
  }

  public static boolean validateInputSchedule(String cronExpression) {
    String[] cronFields = cronExpression.split(" ");
    if (cronFields.length != 6) {
      throw new IllegalArgumentException(
          "refreshSchedule must contain 6 fields in the following format: "
              + "0 minute hour [?*] * daysOfWeek");
    }
    if (!"0".equals(cronFields[0])) {
      throw new IllegalArgumentException("seconds field for refreshSchedule must be set to 0");
    }
    if (!cronFields[1].matches("([0-9]|[1-5][0-9])")) {
      throw new IllegalArgumentException("minute field for refreshSchedule must be a value 0-59");
    }
    if (!cronFields[2].matches("([0-9]|1[0-9]|2[0-3])")) {
      throw new IllegalArgumentException("hour field for refreshSchedule must be a value 0-23");
    }
    if (!"?".equals(cronFields[3]) && !"*".equals(cronFields[3])) {
      throw new IllegalArgumentException(
          "day of month field for refreshSchedule must be '?' or '*'");
    }
    if (!"?".equals(cronFields[4]) && !"*".equals(cronFields[4])) {
      throw new IllegalArgumentException("month field for refreshSchedule must be '?' or '*'");
    }
    if (!validDaysOfWeek(cronFields[5])) {
      throw new IllegalArgumentException("days of week field for refreshSchedule not valid.");
    }
    return true;
  }

  public static boolean validDaysOfWeek(String daysOfWeekExpression) {
    // allow wildcard values
    if ("?".equals(daysOfWeekExpression) || "*".equals(daysOfWeekExpression)) {
      return true;
    }

    String[] daysAbbreviations = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};
    Map<String, Integer> dayAbbreviationsMap = new HashMap<>();

    // Populate map for day abbreviations
    for (int i = 1; i < 8; i++) {
      dayAbbreviationsMap.put(daysAbbreviations[i - 1], i);
    }
    // check if it was specified with a range. There should only be 2 strings if so
    if (daysOfWeekExpression.contains("-")) {
      String[] range = daysOfWeekExpression.split("-");
      if (range.length != 2) {
        throw new IllegalArgumentException(
            "invalid range specified for days of week for refreshSchedule");
      }
      int start = 1;
      int end = 7;
      start = getDayToIntValue(range[0], dayAbbreviationsMap);
      end = getDayToIntValue(range[range.length - 1], dayAbbreviationsMap);
      if (start > end) {
        throw new IllegalArgumentException(
            "range for days of week of refreshSchedule must start low and end high."
                + "SUN = 1, SAT = 7");
      }
    } else {
      // otherwise it's a comma separated list. Check to make sure each value is valid.
      String[] days = daysOfWeekExpression.split(",");
      for (int i = 0; i < days.length; i++) {
        getDayToIntValue(days[i], dayAbbreviationsMap);
      }
    }
    return true;
  }
}

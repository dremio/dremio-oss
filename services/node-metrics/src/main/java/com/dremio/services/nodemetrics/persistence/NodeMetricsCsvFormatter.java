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

import com.dremio.services.nodemetrics.NodeMetrics;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** Serializes node metrics to a format capable of writing as a CSV */
class NodeMetricsCsvFormatter {
  private static final String DATETIME_STR_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  private static final ZoneId TIMEZONE = ZoneId.of("UTC");

  private final Clock clock;

  NodeMetricsCsvFormatter() {
    this(Clock.systemUTC());
  }

  @VisibleForTesting
  NodeMetricsCsvFormatter(Clock clock) {
    this.clock = clock;
  }

  /**
   * Parses the node metrics and formats each NodeMetrics instance to comma-separated values on a
   * newline. Note that field values that may contain commas are quoted using double-quotes. The
   * result only contains values lines and not field names.
   */
  String toCsv(List<NodeMetrics> nodeMetricsList) {
    StringBuilder csvString = new StringBuilder();
    ZonedDateTime utcTimestamp = ZonedDateTime.ofInstant(Instant.now(clock), TIMEZONE);
    for (NodeMetrics nodeMetrics : nodeMetricsList) {
      String line = toCsv(nodeMetrics, utcTimestamp);
      csvString.append(line);
      csvString.append("\n");
    }
    return csvString.toString();
  }

  private String toCsv(NodeMetrics nodeMetrics, ZonedDateTime utcTimestamp) {
    DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern(DATETIME_STR_FORMAT).withZone(TIMEZONE);
    String dateTimeString = dateTimeFormatter.format(utcTimestamp);
    String startTimeString = dateTimeFormatter.format(Instant.ofEpochMilli(nodeMetrics.getStart()));

    // This should be kept in sync with the system table implementation (projected and type-casted
    // fields)
    return String.format(
        "%s,\"%s\",\"%s\",%s,%f,%f,%s,%s,%s,%s,%s",
        dateTimeString,
        nodeMetrics.getName(),
        nodeMetrics.getHost(),
        nodeMetrics.getIp(),
        nodeMetrics.getCpu(),
        nodeMetrics.getMemory(),
        nodeMetrics.getStatus(),
        nodeMetrics.getIsMaster(),
        nodeMetrics.getIsCoordinator(),
        nodeMetrics.getIsExecutor(),
        startTimeString);
  }
}

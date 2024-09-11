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
import java.time.ZonedDateTime;

/** Supports conversions between a datetime and a String representation. */
interface NodeMetricsDateTimeFormatter {
  /** Format a UTC instant as a string capable of being used as a filename. */
  String formatDateTime(Instant instant);

  /**
   * Get a com.dremio.io.file.FileSystem glob pattern for listing files named based on this
   * formatter
   */
  String getDateTimeGlob();

  /** Parses a string returned by formatDateTime into a date time with timezone info */
  ZonedDateTime parseDateTime(String dateTime);
}

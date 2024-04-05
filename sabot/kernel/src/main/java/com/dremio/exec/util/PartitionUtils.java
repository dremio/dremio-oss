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

import java.util.regex.Pattern;

public final class PartitionUtils {

  private PartitionUtils() {}

  /** Standard file system partition column names are of the format "dirN" */
  public static final Pattern DIR_PATTERN = Pattern.compile("dir([0-9]+)");

  /**
   * Test if partition name matches valid format
   *
   * <p>TODO: FILESYSTEM_PARTITION_COLUMN_LABEL should *also* be tested once non-standard names are
   * fully supported
   */
  public static boolean isPartitionName(String partName, boolean useFileSystemNameFilter) {
    if (useFileSystemNameFilter) {
      return DIR_PATTERN.matcher(partName).matches();
    } else {
      return true;
    }
  }
}

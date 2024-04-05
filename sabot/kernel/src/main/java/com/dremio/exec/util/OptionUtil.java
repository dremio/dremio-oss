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

/** Utils functions to derive options from ExecConstants */
public final class OptionUtil {

  private OptionUtil() {}

  public static int getJobCountsAgeInDays(long configuredValue) {
    // Limiting to max of 30, as the job counts are stored for last 30days only in new approach.
    return (int) Math.min(30, configuredValue);
  }
}

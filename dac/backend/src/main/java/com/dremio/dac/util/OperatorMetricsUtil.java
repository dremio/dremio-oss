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

package com.dremio.dac.util;

import java.util.HashMap;

public class OperatorMetricsUtil {
  private static final String[] screenMetrics = {"BYTES_SENT" };

  // Below is the static Map to if we need to custmize the Metrics information.
  public static final HashMap<Integer, String[]> operatorSpecificMetrics = new HashMap<>();

  static {
    operatorSpecificMetrics.put(13, screenMetrics);
  }
}

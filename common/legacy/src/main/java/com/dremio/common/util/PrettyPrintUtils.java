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
package com.dremio.common.util;

import java.text.DecimalFormat;

/** Utilities for generating strings that look nicely formatted */
public class PrettyPrintUtils {
  private static final DecimalFormat dec = new DecimalFormat("0.00");
  private static final DecimalFormat intformat = new DecimalFormat("#,###");

  /** Generate a string from a number representing a quantity of bytes */
  public static String bytePrint(final long size, boolean displayTinyValues) {
    final double t = size / Math.pow(1024, 4);
    if (t > 1) {
      return dec.format(t).concat("TB");
    }

    final double g = size / Math.pow(1024, 3);
    if (g > 1) {
      return dec.format(g).concat("GB");
    }

    final double m = size / Math.pow(1024, 2);
    if (m > 1) {
      return intformat.format(m).concat("MB");
    }

    final double k = size / 1024;
    if (k >= 1) {
      return intformat.format(k).concat("KB");
    }

    // size < 1 KB. Too small to care
    if (displayTinyValues) {
      return "<1 KB";
    }
    return "-";
  }
}

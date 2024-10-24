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

public final class FormattingUtils {
  private static final DecimalFormat decimalFormat = new DecimalFormat("0.00");
  private static final DecimalFormat intFormat = new DecimalFormat("#,###");

  private FormattingUtils() {}

  private static String bytePrint(final long bytes) {
    final double t = bytes / Math.pow(1024, 4);

    if (Math.abs(t) > 1) {
      return decimalFormat.format(t).concat(" TiB");
    }

    final double g = bytes / Math.pow(1024, 3);
    if (Math.abs(g) > 1) {
      return decimalFormat.format(g).concat(" GiB");
    }

    final double m = bytes / Math.pow(1024, 2);
    if (Math.abs(m) > 1) {
      return intFormat.format(m).concat(" MiB");
    }

    final double k = (double) bytes / 1024;

    if (Math.abs(k) >= 1) {
      return intFormat.format(k).concat(" KiB");
    }

    return intFormat.format(k).concat(" bytes");
  }

  public static String formatBytes(long bytes) {
    if (Math.abs(bytes) == 1) {
      return String.format("%d byte", bytes);
    }

    if (Math.abs(bytes) < 1024) {
      return String.format("%d bytes", bytes);
    }

    return String.format("%d bytes (%s)", bytes, bytePrint(bytes));
  }
}

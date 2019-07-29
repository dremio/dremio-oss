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
package com.dremio.dac.server.admin.profile;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

import com.dremio.common.util.PrettyPrintUtils;

/**
 * Abstract class for building profile data
 */
abstract class ProfileBuilder {
  private final NumberFormat format = NumberFormat.getInstance(Locale.US);
  private final DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
  private final DecimalFormat intformat = new DecimalFormat("#,###");

  abstract void appendCell(String string) throws IOException;

  void appendTime(final long d) throws IOException {
    appendCell(getDateFormat().format(d));
  }

  void appendMillis(final long p) throws IOException {
    appendCell(SimpleDurationFormat.format(p));
  }

  void appendNanos(final long p) throws IOException {
    appendMillis(Math.round(p / 1000.0 / 1000.0));
  }

  void appendBytes(final long l) throws IOException {
    appendCell(PrettyPrintUtils.bytePrint(l, false));
  }

  public NumberFormat getFormat() {
    return format;
  }

  public DateFormat getDateFormat() {
    return dateFormat;
  }

  public DecimalFormat getIntformat() {
    return intformat;
  }
}

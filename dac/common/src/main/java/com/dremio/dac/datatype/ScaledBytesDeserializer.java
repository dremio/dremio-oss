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

package com.dremio.dac.datatype;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScaledBytesDeserializer extends StdDeserializer<ScaledBytes> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScaledBytesDeserializer.class);

  private static final Pattern ACCEPTED_PATTERN =
      Pattern.compile("\\s*(\\d+)\\s+([a-z]+)\\s*", Pattern.CASE_INSENSITIVE);

  private static final String EXCEPTION_MESSAGE_PREAMBLE =
      "Expected a binary data size in the form \"`integer` `unit`\", where unit is B, KB, MB, or GB.";

  protected ScaledBytesDeserializer() {
    super(ScaledBytes.class);
  }

  @VisibleForTesting
  public static ScaledBytesDeserializer create() {
    return new ScaledBytesDeserializer();
  }

  @Override
  public ScaledBytes deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    String raw = p.readValueAs(String.class);
    return sanitizeAndConvert(raw);
  }

  private ScaledBytes sanitizeAndConvert(String raw) throws IOException {
    if (null == raw || raw.isEmpty()) {
      final String msg = EXCEPTION_MESSAGE_PREAMBLE + "  " + "Received an empty string instead.";
      throw InvalidFormatException.from(null, msg, raw, ScaledBytes.class);
    }

    Matcher m = ACCEPTED_PATTERN.matcher(raw);

    if (!m.matches()) {
      final String msg =
          String.format(
              "%s  Received \"%s\", which does not match this pattern.",
              EXCEPTION_MESSAGE_PREAMBLE, raw);
      throw InvalidFormatException.from(null, msg, raw, ScaledBytes.class);
    }

    final long quantity;
    try {
      quantity = Long.parseLong(m.group(1));
    } catch (NumberFormatException nfe) {
      final String msg =
          String.format(
              "%s  The first part, \"%s\", is either out-of-range or not parseable as a long.",
              EXCEPTION_MESSAGE_PREAMBLE, m.group(1));
      /*
       * InvalidFormatException cannot take a cause-exception through any of its
       * constructors/factories.  Even though this case seems trivial, swallowed exceptions can cause surprising problems, so log it
       * before losing it.
       */
      LOGGER.debug(
          "Deserializer rejecting non-numeric or out-of-range long value: {}; swallowing NumberFormatException here",
          m.group(1),
          nfe);
      throw InvalidFormatException.from(null, msg, raw, ScaledBytes.class);
    }

    // This is defensive; it should be impossible to get into this block without changing the regex
    if (0 > quantity) {
      final String msg = EXCEPTION_MESSAGE_PREAMBLE + "  " + "The integer cannot be negative.";
      throw InvalidFormatException.from(null, msg, raw, ScaledBytes.class);
    }

    final ScaledBytes.Scale scale = ScaledBytes.getScaleByName(m.group(2));

    if (null == scale) {
      final String msg =
          String.format(
              "%s  \"%s\" is not a supported unit.", EXCEPTION_MESSAGE_PREAMBLE, m.group(2));
      throw InvalidFormatException.from(null, msg, raw, ScaledBytes.class);
    }

    return ScaledBytes.of(quantity, scale);
  }
}

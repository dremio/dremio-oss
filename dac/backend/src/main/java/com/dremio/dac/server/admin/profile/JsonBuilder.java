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

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;

/** Json Builder for Profiles */
public class JsonBuilder extends ProfileBuilder {
  private final JsonGenerator generator;

  JsonBuilder(JsonGenerator generator, final String[] columns) throws IOException {
    this.generator = generator;

    getFormat().setMaximumFractionDigits(3);

    generator.writeStartObject();

    // field names
    generator.writeFieldName("fields");
    generator.writeStartArray();
    for (final String cn : columns) {
      generator.writeString(cn);
    }
    generator.writeEndArray();

    generator.writeFieldName("data");
    generator.writeStartArray();
  }

  public void startEntry() throws IOException {
    generator.writeStartArray();
  }

  public void endEntry() throws IOException {
    generator.writeEndArray();
  }

  public void end() throws IOException {
    generator.writeEndArray();
    generator.writeEndObject();
  }

  public void appendString(String value) throws IOException {
    appendCell(value);
  }

  public void appendInteger(final long l) throws IOException {
    appendCell(Long.toString(l));
  }

  public void appendFormattedInteger(final long n) throws IOException {
    appendCell(getIntformat().format(n));
  }

  public void appendNanosWithUnit(final long p) throws IOException {
    appendCell(SimpleDurationFormat.formatNanos(p));
  }

  public void appendFormattedNumber(final Number n) throws IOException {
    appendCell(getFormat().format(n));
  }

  @Override
  ProfileBuilder appendCell(String value) throws IOException {
    generator.writeString(value);
    return this;
  }
}

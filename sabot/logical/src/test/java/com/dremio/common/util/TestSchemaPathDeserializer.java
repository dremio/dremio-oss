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

import static org.junit.Assert.assertEquals;

import com.dremio.common.expression.SchemaPath;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link SchemaPathDeserializer} */
public class TestSchemaPathDeserializer {

  private final ObjectMapper mapper =
      new ObjectMapper()
          .registerModule(
              new SimpleModule().addDeserializer(SchemaPath.class, new SchemaPathDeserializer()));

  @ParameterizedTest
  @MethodSource("validCases")
  public void test(SchemaPath value) throws JsonProcessingException {
    String serialized = mapper.writeValueAsString(value);
    SchemaPath actual = mapper.readValue(serialized, SchemaPath.class);
    assertEquals(value, actual);
  }

  public static Stream<Arguments> validCases() {
    return Stream.of(
        Arguments.of(SchemaPath.getSimplePath("level1")),
        Arguments.of(SchemaPath.getCompoundPath("level1", "level2")),
        Arguments.of(SchemaPath.getCompoundPath("level1", "escape.needed")),
        Arguments.of(SchemaPath.getCompoundPath("level1", "escape`enclosing")),
        Arguments.of(SchemaPath.getSimplePath("escape`enclosing")),
        Arguments.of(SchemaPath.getSimplePath(".")),
        Arguments.of(SchemaPath.getSimplePath("`")),
        Arguments.of(SchemaPath.getSimplePath("\"")),
        Arguments.of(SchemaPath.getSimplePath("\\")));
  }

  @ParameterizedTest
  @MethodSource("invalidCases")
  public void testErrorScenarios(String serialized, String expectedError)
      throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> mapper.readValue(serialized, SchemaPath.class))
        .hasMessage(expectedError);
  }

  public static Stream<Arguments> invalidCases() {
    return Stream.of(
        Arguments.of("\"``\"", "Invalid format, empty path at 1"),
        Arguments.of("\"`abc`.``\"", "Invalid format, empty path at 7"),
        Arguments.of("\"`abc``cde`\"", "Invalid format, delimiter expected at 5"),
        Arguments.of("\"`abc`.`abc``cde`\"", "Invalid format, delimiter expected at 11"),
        Arguments.of("\"`abc\"", "Invalid format, path segment not closed properly"),
        Arguments.of("\"\"", "No paths found"));
  }
}

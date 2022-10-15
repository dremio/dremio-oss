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
package com.dremio.service.autocomplete.functions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.dremio.service.autocomplete.snippets.Snippet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public final class FunctionOverrides {
  public static final ImmutableMap<String, Function> OVERRIDES = getOverrides();

  private FunctionOverrides() {}

  public static Optional<Function> tryGetOverride(String name) {
    return Optional.ofNullable(OVERRIDES.get(name.toUpperCase()));
  }

  private static ImmutableMap<String, Function> getOverrides() {
    Map<String, Function> overrides = new HashMap<>();
    addGeneralOverrides(overrides);
    addAggregateOverrides(overrides);
    return ImmutableMap.copyOf(overrides);
  }

  private static void addGeneralOverrides(Map<String, Function> overrides) {
    Function characterLength = Function.builder()
      .name("CHARACTER_LENGTH")
      .addSignatures(
        FunctionSignature.builder()
          .returnType(ParameterType.NUMERIC)
          .addParameters(Parameter.builder()
            .type(ParameterType.STRING)
            .kind(ParameterKind.REGULAR)
            .description("The String to take the length of.")
            .build())
          .build())
      .description("Returns the number of characters in a character string")
      .build();

    Function power = Function.builder()
      .name("POWER")
      .addSignatures(
        FunctionSignature.builder()
          .returnType(ParameterType.NUMERIC)
          .addParameters(Parameter.builder()
              .type(ParameterType.NUMERIC)
              .kind(ParameterKind.REGULAR)
              .description("The Base.")
              .build(),
            Parameter.builder()
              .type(ParameterType.NUMERIC)
              .kind(ParameterKind.REGULAR)
              .description("The Exponent.")
              .build())
          .build())
      .description("Returns numeric1 raised to the power of numeric2.")
      .build();

    Function concat = Function.builder()
      .name("CONCAT")
      .addSignatures(
        FunctionSignature.builder()
          .returnType(ParameterType.STRING)
          .addParameters(Parameter.builder()
              .type(ParameterType.STRING)
              .kind(ParameterKind.REGULAR)
              .description("First String.")
              .build(),
            Parameter.builder()
              .type(ParameterType.STRING)
              .kind(ParameterKind.REGULAR)
              .description("Second String.")
              .build(),
            Parameter.builder()
              .type(ParameterType.STRING)
              .kind(ParameterKind.REGULAR)
              .description("Third or More Strings.")
              .build())
          .build())
      .description("Concatenates two or more strings.")
      .build();

    Function substring = Function.builder()
      .name("SUBSTRING")
      .addSignatures(
        FunctionSignature.builder()
          .returnType(ParameterType.CHARACTERS)
          .addParameters(Parameter.builder()
              .type(ParameterType.CHARACTERS)
              .kind(ParameterKind.REGULAR)
              .description("The string to take the substring of.")
              .build(),
            Parameter.builder()
              .type(ParameterType.NUMERIC)
              .kind(ParameterKind.REGULAR)
              .description("The start index.")
              .build())
          .snippetOverride(Snippet.builder()
            .addText("SUBSTRING(")
            .addPlaceholder("string")
            .addText(" FROM ")
            .addPlaceholder("fromIndex")
            .addText(")")
            .build())
          .build(),
        FunctionSignature.builder()
          .returnType(ParameterType.CHARACTERS)
          .addParameters(Parameter.builder()
              .type(ParameterType.CHARACTERS)
              .kind(ParameterKind.REGULAR)
              .description("The string to take the substring of.")
              .build(),
            Parameter.builder()
              .type(ParameterType.NUMERIC)
              .kind(ParameterKind.REGULAR)
              .description("The start index.")
              .build(),
            Parameter.builder()
              .type(ParameterType.NUMERIC)
              .kind(ParameterKind.REGULAR)
              .description("The length.")
              .build())
          .snippetOverride(Snippet.builder()
            .addText("SUBSTRING(")
            .addPlaceholder("string")
            .addText(" FROM ")
            .addPlaceholder("fromIndex")
            .addText(" FOR ")
            .addPlaceholder("forLength")
            .addText(")")
            .build())
          .build())
      .description("Returns the part of a string.")
      .build();

    List<Function> functions = ImmutableList.of(characterLength, power, concat, substring);

    for (Function function : functions) {
      overrides.put(function.getName().toUpperCase(), function);
    }
  }

  private static void addAggregateOverrides(Map<String, Function> overrides) {
    Function count = Function.builder()
      .name("COUNT")
      .addSignatures(
        FunctionSignature.builder()
          .returnType(ParameterType.BIGINT)
          .snippetOverride(Snippet.builder()
            .addText("COUNT(*)")
            .build())
          .build(),
        FunctionSignature.builder()
          .returnType(ParameterType.BIGINT)
          .addParameters(Parameter.builder()
            .type(ParameterType.ANY)
            .kind(ParameterKind.REGULAR)
            .description("The expression to take the count of.")
            .build())
          .snippetOverride(Snippet.builder()
            .addText("COUNT")
            .addText("(")
            .addChoice("ALL", "DISTINCT")
            .addText(" ")
            .addPlaceholder("value")
            .addText(")")
            .build())
          .build())
      .description("Returns the total number of records for the specified expression.")
      .build();

    overrides.put(count.getName().toUpperCase(), count);
  }
}

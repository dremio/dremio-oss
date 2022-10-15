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

package com.dremio.service.autocomplete.functionlist2;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.dremio.service.autocomplete.functions.Function;
import com.dremio.service.autocomplete.functions.FunctionMerger;
import com.dremio.service.autocomplete.functions.FunctionOverrides;
import com.dremio.service.autocomplete.functions.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public final class FunctionListGenerator {
  private final ImmutableMap<String, FunctionSpec> map;

  private final ImmutableList<String> documentedFunctions;

  public Optional<FunctionSpec> tryGetValue(String functionName) {
    return Optional.ofNullable(this.map.get(functionName.toUpperCase()));
  }

  public Collection<String> getKeys() {
    return this.map.keySet();
  }

  public Collection<FunctionSpec> getValues() {
    return this.map.values();
  }

  public ImmutableList<String> getDocumentedFunctions() {
    return this.documentedFunctions;
  }

  private FunctionListGenerator(ImmutableMap<String, FunctionSpec> map, ImmutableList<String> documentedFunctions) {
    Preconditions.checkNotNull(map);
    Preconditions.checkNotNull(documentedFunctions);
    this.map = map;
    this.documentedFunctions = documentedFunctions;
  }

  public static FunctionListGenerator create(List<Function> functions) {
    ImmutableMap<String, FunctionSpec> functionSpecsFromOldFiles = generateFunctionSpecsFromOldSchema();
    Map<String, Function> mergedFunctions = functions
      .stream()
      .collect(
        groupingBy(
          function -> function.getName().toUpperCase(),
          collectingAndThen(
            ImmutableList.toImmutableList(),
            FunctionMerger::merge)));
    ImmutableMap.Builder<String, FunctionSpec> mapBuilder = new ImmutableMap.Builder();
    ImmutableList.Builder<String> documentedFunctionNameBuilder = new ImmutableList.Builder<>();
    for (String functionName : mergedFunctions.keySet()) {
      if (functionSpecsFromOldFiles.get(functionName) != null) {
        mapBuilder.put(functionName, functionSpecsFromOldFiles.get(functionName));
        documentedFunctionNameBuilder.add(functionName);
      } else {
        Function function = FunctionOverrides
          .tryGetOverride(functionName)
          .orElse(mergedFunctions.get(functionName));
        mapBuilder.put(functionName, convertFunctionToFunctionSpec(function));
      }
    }

    return new FunctionListGenerator(mapBuilder.build(), documentedFunctionNameBuilder.build());
  }

  private static ImmutableMap<String, FunctionSpec> generateFunctionSpecsFromOldSchema() {
    ImmutableList<FunctionSpec> functionSpecsFromOldFiles = ImmutableList.of();
    ImmutableMap.Builder<String, FunctionSpec> builder = new ImmutableMap.Builder<>();
    for (FunctionSpec functionSpec : functionSpecsFromOldFiles) {
      builder.put(functionSpec.getName().toUpperCase(), functionSpec);
    }
    return builder.build();
  }

  private static ParameterInfo convertParameterToParameterInfo(Parameter parameter) {
    return parameter.getDescription().isPresent()
      ? ParameterInfo.builder()
        .kind(parameter.getKind())
        .type(parameter.getType())
        .name("<PARAMETER NAME GOES HERE>") // Default value
        .description(parameter.getDescription().get())
        .format("<FORMAT NAME GOES HERE>")
        .build()
      : ParameterInfo.builder()
        .kind(parameter.getKind())
        .type(parameter.getType())
        .name("<PARAMETER NAME GOES HERE>")// Default value
        .description("<DESCRIPTION NAME GOES HERE>")
        .format("<FORMAT NAME GOES HERE>")
      .build();
  }

  private static FunctionSignature convertFunctionSignature(com.dremio.service.autocomplete.functions.FunctionSignature functionSignature) {
    ImmutableList<ParameterInfo> parameters = functionSignature.getParameters()
      .stream()
      .map(parameter -> convertParameterToParameterInfo(parameter))
      .collect(ImmutableList.toImmutableList());

    return FunctionSignature.builder()
      .returnType(functionSignature.getReturnType())
      .addAllParameters(parameters)
      .description("<DESCRIPTION GOES HERE>")
      .sampleCodes(ImmutableList.of(SampleCode.builder()
        .call("<SAMPLE CALL GOES HERE>")
        .result("<SAMPLE RESULT GOES HERE>")
        .build()))
      .build();
  }

  private static FunctionSpec convertFunctionToFunctionSpec(Function function) {
    ImmutableList<FunctionSignature> signatures = function.getSignatures()
      .stream()
      .map(signature -> convertFunctionSignature(signature))
      .collect(ImmutableList.toImmutableList());
    return function.getDescription().isPresent()
      ? FunctionSpec.builder()
      .name(function.getName())
      .addAllSignatures(signatures)
      .dremioVersion("<DREMIO VERSION GOES HERE>")
      .functionCategories(ImmutableList.of())
      .description(function.getDescription())
      .build()
      : FunctionSpec.builder()
      .name(function.getName())
      .addAllSignatures(signatures)
      .dremioVersion("<DREMIO VERSION GOES HERE>")
      .functionCategories(ImmutableList.of())
      .description("<DESCRIPTION GOES HERE>")
      .build();
  }
}

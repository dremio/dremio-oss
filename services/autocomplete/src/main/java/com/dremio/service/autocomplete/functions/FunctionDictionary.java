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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

/**
 * Dictionary of case insensitive function name to SqlFunction.
 */
public final class FunctionDictionary {
  public static final FunctionDictionary INSTANCE = createFromResourceFile("functions.yaml");
  private final ImmutableMap<String, Function> map;

  private FunctionDictionary(ImmutableMap<String, Function> map) {
    Preconditions.checkNotNull(map);
    ImmutableMap.Builder<String, Function> copy = new ImmutableMap.Builder<>();
    for (String key : map.keySet()) {
      copy.put(key.toUpperCase(), map.get(key));
    }

    this.map = copy.build();
  }

  public Optional<Function> tryGetValue(String functionName) {
    return Optional.ofNullable(this.map.get(functionName.toUpperCase()));
  }

  public Collection<Function> getValues() {
    return this.map.values();
  }
  public Collection<String> getKeys() { return this.map.keySet(); }

  public static FunctionDictionary create(
    List<SqlFunction> sqlFunctions,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope,
    boolean enableSignatures) {
    Preconditions.checkNotNull(sqlFunctions);
    Preconditions.checkNotNull(sqlValidator);
    Preconditions.checkNotNull(sqlValidatorScope);

    Map<String, Function> functionsGroupedByName = sqlFunctions
      .stream()
      .map(sqlFunction -> FunctionFactory.createFromSqlFunction(
        sqlFunction,
        sqlValidator,
        sqlValidatorScope,
        enableSignatures))
      .collect(
        groupingBy(
          function -> function.getName().toUpperCase(),
          collectingAndThen(
            toList(),
            Function::mergeOverloads)));

    return new FunctionDictionary(ImmutableMap.copyOf(functionsGroupedByName));
  }

  public static FunctionDictionary createFromResourceFile(String resourcePath) {
    Preconditions.checkNotNull(resourcePath);

    final URL url = Resources.getResource(resourcePath);
    if (url == null) {
      throw new RuntimeException("file not found! " + resourcePath);
    }

    try {
      String yaml = Resources.toString(url, Charsets.UTF_8);
      List<FunctionBaseline> baselines = new ObjectMapper(new YAMLFactory()).registerModule(new GuavaModule()).readValue(
        yaml,
        new TypeReference<List<FunctionBaseline>>() {});

      ImmutableMap.Builder<String, Function> builder = new ImmutableMap.Builder<>();
      for (FunctionBaseline functionBaseline : baselines) {
        builder.put(functionBaseline.input, functionBaseline.getOutput());
      }

      return new FunctionDictionary(ImmutableMap.copyOf(builder.build()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  private static final class FunctionBaseline {
    private final String description;
    private final String input;
    private final Function output;

    @JsonCreator
    public FunctionBaseline(
      @JsonProperty("description") String description,
      @JsonProperty("input") String input,
      @JsonProperty("output") Function output) {
      this.description = description;
      this.input = input;
      this.output = output;
    }

    public String getDescription() {
      return description;
    }

    public String getInput() {
      return input;
    }

    public Function getOutput() {
      return output;
    }
  }
}

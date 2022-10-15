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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.junit.Test;

import com.dremio.service.autocomplete.OperatorTableFactory;
import com.dremio.service.autocomplete.functions.FunctionFactory;
import com.dremio.service.autocomplete.functions.ParameterResolverTests;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;

public final class FunctionListGeneratorTests {
  private static final SqlOperatorTable OPERATOR_TABLE = OperatorTableFactory.createWithProductionFunctions(ParameterResolverTests.FUNCTIONS);

@Test
  public void tests() throws IOException {
    FunctionListGenerator functionListGenerator = FunctionListGenerator.create(OPERATOR_TABLE
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
      .map(sqlOperator -> (SqlFunction) sqlOperator)
      .map(sqlFunction -> FunctionFactory.create(sqlFunction))
      .collect(Collectors.toList()));

    Set<String> names = OperatorTableFactory.createWithProductionFunctions(ImmutableList.of())
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
      .map(sqlOperator -> sqlOperator.getName().toUpperCase())
      .collect(Collectors.toSet());

    for(String documentedFunctions : functionListGenerator.getDocumentedFunctions()) {
      writeFunction(functionListGenerator.tryGetValue(documentedFunctions).get(), true);
    }

    Set<String> documentedFunctionNames = new HashSet(functionListGenerator.getDocumentedFunctions());
    Set<String> undocumentedFunctionNames = Sets.difference(names, documentedFunctionNames);
    for(String functionNames : undocumentedFunctionNames) {
      writeFunction(functionListGenerator.tryGetValue(functionNames).get(), false);
    }
  }

  private static void writeFunction(FunctionSpec functionSpec, boolean documented) throws IOException {
    Path goldenFileActualPath = Paths.get("target",
      "goldenfiles",
      "actual",
      "function_specs",
      documented ? "documented" : "undocumented",
      functionSpec.getName() + ".yaml");

    try {
      Files.createDirectories(goldenFileActualPath.getParent());
    } catch (FileAlreadyExistsException exception) {
      //Do nothing
    }

    try {
      Files.createFile(Files.createFile(goldenFileActualPath));
    } catch (FileAlreadyExistsException exception) {
      //Do nothing
    }

    ObjectMapper objectMapper = new ObjectMapper(
      new YAMLFactory()
        .disable(YAMLGenerator.Feature.SPLIT_LINES)
        .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
        .enable(YAMLGenerator.Feature.INDENT_ARRAYS))
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .registerModule(new JavaTimeModule())
      .registerModule(new GuavaModule())
      .registerModule(new Jdk8Module());
    objectMapper.writeValue(
      new File(goldenFileActualPath.toUri().getPath()),
      functionSpec);

    // Prepend the license header
    Path LICENSE_HEADER_PATH = Paths.get(Resources.getResource("goldenfiles/header.txt").getPath());
    String fileContent = new String(Files.readAllBytes(goldenFileActualPath));
    String licenseHeaderContent = new String(Files.readAllBytes(LICENSE_HEADER_PATH));
    String fileContentWithLicence = licenseHeaderContent + '\n' + fileContent;
    Files.write(goldenFileActualPath, fileContentWithLicence.getBytes(StandardCharsets.UTF_8));
  }
}

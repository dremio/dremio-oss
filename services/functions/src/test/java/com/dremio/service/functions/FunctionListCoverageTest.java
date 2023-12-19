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
package com.dremio.service.functions;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;
import com.dremio.service.functions.generator.FunctionFactory;
import com.dremio.service.functions.generator.FunctionMerger;
import com.dremio.service.functions.model.Function;
import com.dremio.service.functions.model.FunctionSignature;
import com.dremio.service.functions.model.Parameter;
import com.dremio.service.functions.model.SampleCode;
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

/**
 * Test to assert that all the functions in the system are in one of two states:
 * 1) Documented
 * 2) To Be Documented
 *
 * If a new function is detected, then we create a yaml spec for it and the developer is responsible for one of:
 * 1) Documenting the function and moving it to function_specs/documented or
 * 2) Just moving the function to function_specs/undocumented and let the doc writers know.
 */
public final class FunctionListCoverageTest {
  private static final SabotConfig SABOT_CONFIG = SabotConfig.create();
  private static final ScanResult SCAN_RESULT = ClassPathScanner.fromPrescan(SABOT_CONFIG);
  private static final FunctionImplementationRegistry FUNCTION_IMPLEMENTATION_REGISTRY = FunctionImplementationRegistry.create(
    SABOT_CONFIG,
    SCAN_RESULT);
  private static final SqlOperatorTable OPERATOR_TABLE = DremioCompositeSqlOperatorTable.create(FUNCTION_IMPLEMENTATION_REGISTRY);

  private static final FunctionFactory FUNCTION_FACTORY = FunctionFactory.makeFunctionFactory(OPERATOR_TABLE);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(
      new YAMLFactory()
        .disable(YAMLGenerator.Feature.SPLIT_LINES)
        .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
        .enable(YAMLGenerator.Feature.INDENT_ARRAYS))
    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .registerModule(new JavaTimeModule())
    .registerModule(new GuavaModule())
    .registerModule(new Jdk8Module());

  @Test
  public void assertCoverage() throws IOException {
    Set<String> allFunctions = getAllFunctions();
    Set<String> knownFunctions = FunctionListDictionary.getFunctionNames();
    for (String knownFunction : knownFunctions.stream().sorted().collect(Collectors.toList())) {
      String reason = ExcludedFunctions.reasonForExcludingFunction(knownFunction);
      Assert.assertNull(reason);
    }

    // These are functions that are new to the system and need to be documented or at least marked as undocumented.
    Set<String> newToSystem = Sets.difference(allFunctions, knownFunctions);
    if (!newToSystem.isEmpty()) {
      for (String name : newToSystem) {
        Function functionSpec = FunctionMerger
          .merge(
            OPERATOR_TABLE
              .getOperatorList()
              .stream()
              .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
              .map(sqlOperator -> (SqlFunction) sqlOperator)
              .filter(sqlFunction -> sqlFunction.getName().equalsIgnoreCase(name))
              .map(FUNCTION_FACTORY::fromSqlFunction)
              .collect(ImmutableList.toImmutableList()));
        writeFunction(functionSpec);
      }

      Assert.fail("" +
        "Functions that are neither documented nor undocumented have been detected.\n" +
        "Take the yaml files in services/functions/target/missing_function_specs \n" +
        "and move to either services/functions/src/main/resources/function_specs/documented \n" +
        "or services/functions/src/main/resources/function_specs/undocumented.\n" +
        "The following are new to the system: " + String.join("\n", newToSystem.stream().sorted().collect(Collectors.toList())));
    }
  }

  private static Set<String> getAllFunctions() {
    return OPERATOR_TABLE
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
      .map(sqlOperator -> (SqlFunction) sqlOperator)
      .map(sqlOperator -> sqlOperator.getName().toUpperCase())
      .filter(functionName -> !ExcludedFunctions.shouldExcludeFunction(functionName))
      .collect(Collectors.toSet());
  }

  private static void writeFunction(Function functionSpec) throws IOException {
    functionSpec = populateDefaults(functionSpec);

    Path missingFunctionSpecPath = Paths.get("target",
      "missing_function_specs",
      functionSpec.getName() + ".yaml");

    try {
      Files.createDirectories(missingFunctionSpecPath.getParent());
    } catch (FileAlreadyExistsException exception) {
      //Do nothing
    }

    try {
      Files.createFile(Files.createFile(missingFunctionSpecPath));
    } catch (FileAlreadyExistsException exception) {
      //Do nothing
    }

    OBJECT_MAPPER.writeValue(
      new File(missingFunctionSpecPath.toUri().getPath()),
      functionSpec);

    // Prepend the license header
    Path LICENSE_HEADER_PATH = Paths.get(Resources.getResource("goldenfiles/header.txt").getPath());
    String fileContent = new String(Files.readAllBytes(missingFunctionSpecPath));
    String licenseHeaderContent = new String(Files.readAllBytes(LICENSE_HEADER_PATH));
    String fileContentWithLicence = licenseHeaderContent + '\n' + fileContent;
    Files.write(missingFunctionSpecPath, fileContentWithLicence.getBytes(StandardCharsets.UTF_8));
  }

  private static Function populateDefaults(Function function) {
    return Function.builder()
      .name(function.getName())
      .addAllSignatures(
        function
          .getSignatures()
          .stream()
          .map(FunctionListCoverageTest::populateDefaults)
          .collect(Collectors.toList()))
      .description("<DESCRIPTION GOES HERE>")
      .functionCategories(ImmutableList.of())
      .build();
  }

  private static FunctionSignature populateDefaults(FunctionSignature functionSignature) {
    return FunctionSignature.builder()
      .returnType(functionSignature.getReturnType())
      .addAllParameters(
        functionSignature
          .getParameters()
          .stream()
          .map(FunctionListCoverageTest::populateDefaults)
          .collect(Collectors.toList()))
      .description("<DESCRIPTION GOES HERE>")
      .sampleCodes(
        ImmutableList.of(
          SampleCode.create(
            "<SAMPLE CALL GOES HERE>",
            "<SAMPLE RETURN GOES HERE>")))
      .build();
  }

  private static Parameter populateDefaults(Parameter parameter) {
    return Parameter.builder()
      .kind(parameter.getKind())
      .type(parameter.getType())
      .name("<PARAMETER NAME GOES HERE>")
      .format("<PARAMETER FORMAT GOES HERE>")
      .description("<PARAMETER DESCRIPTION GOES HERE>")
      .build();
  }
}

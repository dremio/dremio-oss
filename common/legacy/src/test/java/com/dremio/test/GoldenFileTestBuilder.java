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
package com.dremio.test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

/**
 * Class generating golden files used for baseline / data-driven testing
 */
public final class GoldenFileTestBuilder<I, O, I_W> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GoldenFileTestBuilder.class);
  private static final Path LICENSE_HEADER_PATH = Paths.get(Resources.getResource("goldenfiles/header.txt").getPath());
  private static final ObjectMapper objectMapperForComparison = createObjectMapper();
  private static final ObjectMapper objectMapper = createObjectMapper();

  private final List<DescriptionAndInput<I>> descriptionAndInputs = new ArrayList<>();
  private final ThrowingFunction<I, O> executeTestFunction;
  private Function<Throwable, String> exceptionSerializer;
  private Function<I, I> executeTestFunctionInputDeserializer;
  private Function<I, I_W> inputSerializer;
  private Function<List<O>, String> outputSummarySerializer;
  private Function<Map<String, Object>, O> mapToOutputObject;
  private Map<String, String> outputComparisonReplacements;
  private boolean allowUnorderedMatch;
  private final String parameterizedInputTag;

  public <X, Y> GoldenFileTestBuilder(
    ThrowingFunction<I, O> executeTestFunction,
    Function<I, I_W> inputSerializer) {
    this(executeTestFunction, inputSerializer, null);
  }

  public <X, Y> GoldenFileTestBuilder(
    ThrowingFunction<I, O> executeTestFunction,
    Function<I, I_W> inputSerializer,
    String parameterizedInputTag) {
    this.executeTestFunction = executeTestFunction;
    this.executeTestFunctionInputDeserializer = input -> input;
    this.inputSerializer = inputSerializer;
    this.outputSummarySerializer = input -> null;
    this.outputComparisonReplacements = new HashMap<>();
    this.allowUnorderedMatch = false;
    this.parameterizedInputTag = parameterizedInputTag;
  }

  public GoldenFileTestBuilder<I, O, I_W> add(String description, I input) {
    this.descriptionAndInputs.add(new DescriptionAndInput<I>(description, input, false));
    return this;
  }

  public GoldenFileTestBuilder<I, O, I_W> addButIgnore(String description, I input) {
    this.descriptionAndInputs.add(new DescriptionAndInput<I>(description, input, true));
    return this;
  }

  public <T> GoldenFileTestBuilder<I, O, I_W> addListByRule(List<T> list, Function<T, Pair<String, I>> rule) {
    for (T item : list) {
      Pair<String, I> output = rule.apply(item);
      String description = output.getLeft();
      I input = output.getRight();
      add(description, input);
    }

    return this;
  }

  public GoldenFileTestBuilder<I, O, I_W> setExceptionSerializer(Function<Throwable, String> exceptionSerializer) {
    this.exceptionSerializer = exceptionSerializer;
    return this;
  }

  public GoldenFileTestBuilder<I, O, I_W> setExecuteTestFunctionInputDeserializer(Function<I, I> executeTestFunctionInputDeserializer) {
    this.executeTestFunctionInputDeserializer = executeTestFunctionInputDeserializer;
    return this;
  }


  public GoldenFileTestBuilder<I, O, I_W> setOutputSummarySerializer(Function<List<O>, String> outputSummarySerializer) {
    this.outputSummarySerializer = outputSummarySerializer;
    return this;
  }


  public GoldenFileTestBuilder<I, O, I_W> setOutputComparisonReplacements(Map<String, String> outputComparisonReplacements) {
    this.outputComparisonReplacements = outputComparisonReplacements;
    return this;
  }

  public GoldenFileTestBuilder<I, O, I_W> setMapToOutputObject(Function<Map<String, Object>, O> mapToOutputObject) {
    this.mapToOutputObject = mapToOutputObject;
    return this;
  }

  public GoldenFileTestBuilder<I, O, I_W> allowExceptions() {
    return setExceptionSerializer(GoldenFileTestBuilder::defaultExceptionSerializer);
  }

  public GoldenFileTestBuilder<I, O, I_W> allowUnorderedMatch() {
    allowUnorderedMatch = true;
    return this;
  }

  public  GoldenFileTestBuilder<I, O, I_W> ignoreOutputFieldsForComparison(Class<?> target, Class<?> mixinSource) {
    objectMapperForComparison.addMixInAnnotations(target, mixinSource);
    return this;
  }

  public  GoldenFileTestBuilder<I, O, I_W> disableSerializationOrderByKeys() {
    objectMapperForComparison.disable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    return this;
  }

  public void runTests() {
    try {
      Preconditions.checkState(!descriptionAndInputs.isEmpty(), "No test cases found.");
      // Generate the Input and Output pairs
      List<InputAndOutput<I_W, O>> actualInputAndOutputList = new ArrayList<>();
      List<O> actualOutputList = new ArrayList<>();
      for (DescriptionAndInput<I> descriptionAndInput : descriptionAndInputs) {
        InputAndOutput<I_W, O> inputAndOutput;
        I_W inputForSerialization = inputSerializer.apply(descriptionAndInput.input);
        I inputForExecuteTestFunction = executeTestFunctionInputDeserializer.apply(descriptionAndInput.input);

        try {
          inputAndOutput = InputAndOutput.createSuccess(
              descriptionAndInput.description,
              inputForSerialization,
              executeTestFunction.apply(inputForExecuteTestFunction));
        } catch (Throwable t) {
          if (exceptionSerializer == null) {
            throw new RuntimeException(descriptionAndInput.description, t);
          }

          inputAndOutput = InputAndOutput.createFailure(
              descriptionAndInput.description,
              inputForSerialization,
              t,
              exceptionSerializer);
        }

        actualInputAndOutputList.add(inputAndOutput);
        actualOutputList.add(inputAndOutput.output);
      }

      // Write the actual values, so user's can diff with the expected and overwrite the golden file if the change is acceptable.
      Path goldenFileActualPath = getGoldenFileActualPath();
      writeActualGoldenFile(goldenFileActualPath, actualInputAndOutputList, outputSummarySerializer.apply(actualOutputList));

      List<InputAndOutput<I_W, O>> expectedInputAndOutputList = readExpectedFile();

      // Assert equality
      assertGoldenFilesAreEqual(expectedInputAndOutputList, actualInputAndOutputList);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private List<InputAndOutput<I_W, O>> readExpectedFile() {
    String path = goldenFileResource();
    try {
      List<InputAndOutput<I_W, O>> inputAndOutputs = objectMapper.readValue(
          Resources.getResource(path),
          new TypeReference<List<InputAndOutput<I_W, O>>>(){});

      // objectMapper.readValue produces a map instead of an output object,
      // which is why mapToOutputObject is used here
      if (mapToOutputObject != null) {
        for (InputAndOutput<I_W, O> inputAndOutput : inputAndOutputs) {
          if (inputAndOutput.output != null) {
            O outputObject = mapToOutputObject.apply((Map<String, Object>) inputAndOutput.output);
            inputAndOutput.output = outputObject;
          }
        }
      }
      return inputAndOutputs;
    } catch(IllegalArgumentException|IOException ex) {
      LOGGER.error("Exception while read expected file", ex);
      return ImmutableList.of(); //Return empty list so file is generated for the first run.
    }
  }

  public String findFileName() {
    Pair<String, String> callingClassAndMethod = findCallingTestClassAndMethod();
    return callingClassAndMethod.getLeft() + "." + callingClassAndMethod.getRight() +
      ((parameterizedInputTag == null) ? "" : "." + parameterizedInputTag);
  }

  private Pair<String, String> findCallingTestClassAndMethod() {
    StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
    for (int i = 1; i < stElements.length; i++) {
      StackTraceElement ste = stElements[i];
      if (ste.getClassName().equals(GoldenFileTestBuilder.class.getName())) {
        continue;
      } else if (ste.getClassName().indexOf("java.lang.Thread") == 0) {
        continue;
      }

      try {
        Class<?> clazz = Class.forName(ste.getClassName());
        for (Method method : clazz.getMethods()) {
          if (method.getName().equals(ste.getMethodName())
            && (method.getDeclaredAnnotation(Test.class) != null || method.getDeclaredAnnotation(ParameterizedTest.class) != null)) {
            String[] classNamespaceTokens = ste.getClassName().split("\\.");
            String testClassName = classNamespaceTokens[classNamespaceTokens.length - 1];
            String methodName = ste.getMethodName();

            return Pair.of(testClassName, methodName);
          }
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException("No @Test method found");
  }

  private Path getGoldenFileActualPath() throws IOException {
    return Paths.get("target","goldenfiles", "actual", findFileName() + ".yaml");
  }

  public String goldenFileResource() {
    return "goldenfiles/expected/" + findFileName() + ".yaml";
  }

  public String inputFileResource() {
    return "goldenfiles/input/" + findFileName() + ".yaml";
  }

  private String messageToFix() {
    try {
      String actualPath = getGoldenFileActualPath().toString();
      String goldenPath = "src/test/resources/" + goldenFileResource();
      return ""
          + "To fix:\n"
          + "\t`cp " + actualPath+ " " + goldenPath + "`\n"
          + "To Diff:\n"
          + "\t`sdiff " + actualPath+ " " + goldenPath + "`\n";
    } catch (IOException exception) {
      return null;
    }
  }

  public static <I, O> GoldenFileTestBuilder<I, O, I> create(ThrowingFunction<I, O> executeTestFunction) {
    return new GoldenFileTestBuilder<>(executeTestFunction, i -> i);
  }

  public static <I, O> GoldenFileTestBuilder<I, O, I> create(
    ThrowingFunction<I, O> executeTestFunction, Function<I, I> inputSerializer) {
    return new GoldenFileTestBuilder<>(executeTestFunction, inputSerializer);
  }

  private static <I, O> void writeActualGoldenFile(
    Path goldenFileActualPath,
    List<InputAndOutput<I, O>> actualInputAndOutputList, String outputSummary) throws IOException {
    try {
      Files.createDirectories(goldenFileActualPath.getParent());
    } catch (FileAlreadyExistsException exception) {
      // Do Nothing.
    }

    try {
      Files.createFile(Files.createFile(goldenFileActualPath));
    } catch (FileAlreadyExistsException exception) {
      // Do Nothing.
    }

    objectMapper.writeValue(
      new File(goldenFileActualPath.toUri().getPath()),
      actualInputAndOutputList);

    // Prepend the license header
    String fileContent = new String(Files.readAllBytes(goldenFileActualPath));
    String licenseHeaderContent = new String(Files.readAllBytes(LICENSE_HEADER_PATH));
    if (outputSummary == null) {
      outputSummary = "";
    }

    String fileContentWithLicence = licenseHeaderContent + '\n' + fileContent + '\n' + outputSummary;
    if (outputSummary == null) {
      outputSummary = "";
    }

    Files.write(goldenFileActualPath, fileContentWithLicence.getBytes(StandardCharsets.UTF_8));
  }

  private void assertGoldenFilesAreEqual(
    List<InputAndOutput<I_W, O>> expectedInputAndOutputList,
    List<InputAndOutput<I_W, O>> actualInputAndOutputList) throws JsonProcessingException {
    String messageToFix = messageToFix();
    Assert.assertEquals(messageToFix, expectedInputAndOutputList.size(), actualInputAndOutputList.size());

    for (int i = 0; i < expectedInputAndOutputList.size(); i++) {
      InputAndOutput expectedInputAndOutput = expectedInputAndOutputList.get(i);
      InputAndOutput actualInputAndOutput = actualInputAndOutputList.get(i);
      DescriptionAndInput<I> descriptionAndInput = descriptionAndInputs.get(i);

      if (!descriptionAndInput.ignore) {
        Assert.assertEquals(
          "Descriptions differ,\n" + messageToFix,
          expectedInputAndOutput.description,
          actualInputAndOutput.description);
        String expectedInputString = objectMapperForComparison.writeValueAsString(expectedInputAndOutput.input);
        String actualInputString = objectMapperForComparison.writeValueAsString(actualInputAndOutput.input);
        Assert.assertEquals(
          "Inputs for baseline differ,\n" + messageToFix,
          expectedInputString,
          actualInputString);

        Assert.assertEquals(
          "Exception Message for baselines differ, \n" + messageToFix + " with input " + expectedInputString,
          expectedInputAndOutput.exceptionMessage == null ? null : applyReplacements(expectedInputAndOutput.exceptionMessage.toString()),
          actualInputAndOutput.exceptionMessage  == null ? null : applyReplacements(actualInputAndOutput.exceptionMessage.toString()));

        String expectedOutputString = applyReplacements(objectMapperForComparison.writeValueAsString(expectedInputAndOutput.output));
        String actualOutputString = applyReplacements(objectMapperForComparison.writeValueAsString(actualInputAndOutput.output));
        if (!expectedOutputString.equals(actualOutputString)) {
          if (allowUnorderedMatch) {
            if (!isPermutation(expectedOutputString, actualOutputString)) {
              Assert.assertEquals(
                "Outputs for baselines differ,\n" + messageToFix + " with input " + expectedInputString,
                expectedOutputString,
                actualOutputString);
            }
          } else {
            Assert.assertEquals(
              "Outputs for baselines differ,\n" + messageToFix + " with input " + expectedInputString,
              expectedOutputString,
              actualOutputString);
          }
        }
      }
    }
  }

  public static boolean isPermutation(String str1, String str2) {
    if (str1.length() != str2.length()) {
      return false;
    }

    Map<Character, Integer> map1 = new HashMap<>();
    Map<Character, Integer> map2 = new HashMap<>();

    for (int i = 0; i < str1.length(); i++) {
      char c1 = str1.charAt(i);
      char c2 = str2.charAt(i);
      map1.put(c1, map1.getOrDefault(c1, 0) + 1);
      map2.put(c2, map2.getOrDefault(c2, 0) + 1);
    }

    return map1.equals(map2);
  }

  private String applyReplacements(String input) {
    String output = input;
    for (Map.Entry<String, String> entry : outputComparisonReplacements.entrySet()) {
      String pattern = entry.getKey();
      String replacement = entry.getValue();

      output = output.replaceAll(pattern, replacement);
    }

    return output;
  }

  public static String redactedExceptionSerializer(Throwable throwable) {
    // We need to remove information from the exception message that changes from run to run:
    String exceptionMessage = throwable.getMessage();

    // The query text could have a from clause with a path relative to the user name.
    int sqlQueryIndex = exceptionMessage.indexOf("SQL Query");
    if (sqlQueryIndex > 0) {
      int endOfQueryIndex = exceptionMessage.indexOf("[Error Id");
      if (endOfQueryIndex > 0) {
        exceptionMessage = exceptionMessage.substring(0, sqlQueryIndex)
                           + "[QUERY REDACTED FOR BASELINES]\n"
                           + exceptionMessage.substring(endOfQueryIndex);
      }
    }

    // Some of the dremio exception messages dumps an entire stack trace with unique error id in the message
    // Which can't go in the baseline, since it changes from run to run.
    int errorIdIndex = exceptionMessage.indexOf("[Error Id");
    if (errorIdIndex > 0) {
      exceptionMessage = exceptionMessage.substring(0, errorIdIndex);
    }

    return exceptionMessage;
  }

  private static String defaultExceptionSerializer(Throwable throwable) {
    return throwable.getMessage();
  }

  @FunctionalInterface
  public interface ThrowingFunction<T,R> {
    R apply(T t) throws Exception;
  }

  private static final class DescriptionAndInput<I> {
    private final String description;
    private final I input;
    private final boolean ignore;

    private DescriptionAndInput(String description, I input, boolean ignore) {
      assert description != null;
      assert input != null;

      this.description = description;
      this.input = input;
      this.ignore = ignore;
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  public static final class InputAndOutput<I, O> {
    public final String description;
    public final I input;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public O output;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final MultiLineString exceptionMessage;

    @JsonCreator
    private InputAndOutput(
      @JsonProperty("description") String description,
      @JsonProperty("input") I input,
      @JsonProperty("output") O output,
      @JsonProperty("exceptionMessage") MultiLineString exceptionMessage) {
      this.description = description;
      this.input = input;
      this.output = output;
      this.exceptionMessage = exceptionMessage;
    }

    public static <I, O> InputAndOutput createSuccess(String description, I input, O output) {
      return new InputAndOutput(description, input, output, null);
    }

    public static <I, O> InputAndOutput createFailure(
      String description, I input,
      Throwable throwable,
      Function<Throwable, String> exceptionSerializer) {
      String exceptionMessage = exceptionSerializer.apply(throwable);
      return new InputAndOutput(
        description,
        input,
        null,
        MultiLineString.create(exceptionMessage));
    }
  }

  /**
   * Serializes a byte array as a base64 string.
   */
  @JsonSerialize(using = Base64StringSerializer.class)
  @JsonDeserialize(using = Base64StringDeserializer.class)
  public static final class Base64String {
    private final byte[] bytes;

    private Base64String(byte[] bytes) {
      Preconditions.checkNotNull(bytes);
      this.bytes = bytes;
    }

    public byte[] getBytes() {
      return this.bytes;
    }

    public static Base64String create(byte[] bytes) {
      return new Base64String(bytes);
    }
  }

  private static final class Base64StringSerializer extends StdSerializer<Base64String> {
    public Base64StringSerializer() {
      this(null);
    }

    public Base64StringSerializer(Class<Base64String> t) {
      super(t);
    }

    @Override
    public void serialize(
      Base64String value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
      jgen.writeString(Base64.getEncoder().encodeToString(value.bytes));
    }
  }

  private static final class Base64StringDeserializer extends StdDeserializer<Base64String> {
    public Base64StringDeserializer() {
      this(null);
    }

    public Base64StringDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Base64String deserialize(JsonParser jp, DeserializationContext ctxt)
      throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);

      assert node.isTextual();
      byte[] bytes = Base64.getDecoder().decode(node.asText());
      return new Base64String(bytes);
    }
  }

  /**
   * Serializes a string with newlines as a array of strings (1 for each line),
   * So that the yaml serializer is forced to display the results correctly
   */
  @JsonSerialize(using = MultiLineStringSerializer.class)
  @JsonDeserialize(using = MultiLineStringDeserializer.class)
  public static final class MultiLineString {
    public final String[] lines;

    public MultiLineString(String[] lines) {
      Preconditions.checkNotNull(lines);
      Preconditions.checkArgument(lines.length != 0);
      this.lines = lines;
    }

    public MultiLineString(List<String> lineList) {
      Preconditions.checkNotNull(lineList);
      Preconditions.checkArgument(lineList.size() != 0);
      this.lines = lineList.stream().toArray(String[] ::new);
    }

    public static MultiLineString create(String value) {
      Preconditions.checkNotNull(value);
      return new MultiLineString(value.split("\\R"));
    }

    @Override
    public String toString() {
      return String.join("\n", this.lines);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      MultiLineString that = (MultiLineString) o;
      return Arrays.equals(lines, that.lines);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(lines);
    }
  }

  private static final class MultiLineStringSerializer extends StdSerializer<MultiLineString> {
    public MultiLineStringSerializer() {
      this(null);
    }

    public MultiLineStringSerializer(Class<MultiLineString> t) {
      super(t);
    }

    @Override
    public void serialize(
      MultiLineString value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
      if (value.lines.length == 1) {
        jgen.writeString(value.lines[0]);
      } else {
        jgen.writeArray(value.lines, 0, value.lines.length);
      }
    }
  }

  private static ObjectMapper createObjectMapper(){
    LoaderOptions loaderOptions = new LoaderOptions();
    loaderOptions.setCodePointLimit(10 * 1024 * 1024); // Set loader option to load a file as large as 10 MB
    return new ObjectMapper(
      YAMLFactory.builder()
        .loaderOptions(loaderOptions)
        .disable(YAMLGenerator.Feature.SPLIT_LINES)
        .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
        .enable(YAMLGenerator.Feature.INDENT_ARRAYS)
        .build())
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .registerModule(new JavaTimeModule())
      .registerModule(new GuavaModule())
      .registerModule(new Jdk8Module());
  }

  private static final class MultiLineStringDeserializer extends StdDeserializer<MultiLineString> {
    public MultiLineStringDeserializer() {
      this(null);
    }

    public MultiLineStringDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public MultiLineString deserialize(JsonParser jp, DeserializationContext ctxt)
      throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);

      MultiLineString multiLineString;
      if (node.isTextual()) {
        multiLineString =  MultiLineString.create(node.asText());
      } else if (node.isArray()) {
        List<String> lines = new ArrayList<>();
        Iterator<JsonNode> iterator = node.iterator();
        while (iterator.hasNext()) {
          JsonNode element = iterator.next();
          lines.add(element.asText());
        }

        multiLineString = new MultiLineString(lines.stream().toArray(String[]::new));
      } else {
        throw new RuntimeException("Unexpected type: " + node.getNodeType());
      }

      return multiLineString;
    }
  }
}

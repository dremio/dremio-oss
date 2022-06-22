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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

/**
 * Class generating golden files used for baseline / data-driven testing
 */
public final class GoldenFileTestBuilder<I, O> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GoldenFileTestBuilder.class);
  private static final Path LICENSE_HEADER_PATH = Paths.get(Resources.getResource("goldenfiles/header.txt").getPath());
  private static final ObjectMapper objectMapper = getObjectMapper();

  private final ThrowingFunction<I, O> executeTestFunction;
  private final List<DescriptionAndInput<I>> descriptionAndInputs;
  boolean allowExceptions;
  boolean showFullStackTrace;

  public GoldenFileTestBuilder(ThrowingFunction<I, O> executeTestFunction) {
    this.executeTestFunction = executeTestFunction;
    this.descriptionAndInputs = new ArrayList<>();
  }

  public GoldenFileTestBuilder<I, O> allowExceptions() {
    this.allowExceptions = true;
    return this;
  }

  public GoldenFileTestBuilder<I, O> showFullStackTrace() {
    this.showFullStackTrace = true;
    return this;
  }

  public GoldenFileTestBuilder<I, O> add(String description, I input) {
    this.descriptionAndInputs.add(new DescriptionAndInput<I>(description, input));
    return this;
  }

  public void runTests() {
    try {
      Preconditions.checkState(!descriptionAndInputs.isEmpty(), "No test cases found.");
      // Generate the Input and Output pairs
      List<InputAndOutput<I, O>> actualInputAndOutputList = new ArrayList<>();
      for (DescriptionAndInput<I> descriptionAndInput : this.descriptionAndInputs) {
        InputAndOutput<I, O> inputAndOutput;
        try {
          inputAndOutput = InputAndOutput.createSuccess(
              descriptionAndInput.description,
              descriptionAndInput.input,
              this.executeTestFunction.apply(descriptionAndInput.input));
        } catch (Throwable t) {
          if (this.allowExceptions) {
            inputAndOutput = InputAndOutput.createFailure(
                descriptionAndInput.description,
                descriptionAndInput.input,
                t,
                this.showFullStackTrace);
          } else {
            throw new RuntimeException(t);
          }
        }

        actualInputAndOutputList.add(inputAndOutput);
      }

      // Calculate the file name
      String callingMethodName = GoldenFileTestBuilder.getCallingMethodName();
      String callingClassName = GoldenFileTestBuilder.getCallingClassName();
      String fileName = callingClassName + "." + callingMethodName;

      // Write the actual values, so user's can diff with the expected and overwrite the golden file if the change is acceptable.
      Path goldenFileActualPath = getGoldenFileActualPath(fileName);
      writeActualGoldenFile(goldenFileActualPath, actualInputAndOutputList);

      List<InputAndOutput<I, O>> expectedInputAndOutputList = readExpectedFile(fileName);

      // Assert equality
      assertGoldenFilesAreEqual(fileName, expectedInputAndOutputList, actualInputAndOutputList);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private List<InputAndOutput<I, O>> readExpectedFile(String fileName) {
    String path = getGoldenFileResource(fileName);
    try {
      return objectMapper.readValue(
          Resources.getResource(path),
          new TypeReference<List<InputAndOutput<I, O>>>(){});
    } catch(IllegalArgumentException|IOException ex) {
      LOGGER.error("Exception while read expected file", ex);
      return ImmutableList.of(); //Return empty list so file is generated for the first run.
    }
  }

  private static String getCallingMethodName() {
    StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
    StackTraceElement e = stacktrace[3];//maybe this number needs to be corrected
    String methodName = e.getMethodName();
    return methodName;
  }

  private static String getCallingClassName() {
    StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
    for (int i=1; i<stElements.length; i++) {
      StackTraceElement ste = stElements[i];
      if (!ste.getClassName().equals(GoldenFileTestBuilder.class.getName()) && ste.getClassName().indexOf("java.lang.Thread")!=0) {
        String[] classNamespaceTokens = ste.getClassName().split("\\.");
        return classNamespaceTokens[classNamespaceTokens.length - 1];
      }
    }

    return null;
  }

  private static Path getGoldenFileActualPath(String fileName) throws IOException {
    return Paths.get("target","goldenfiles", "actual", fileName + ".yaml");
  }

  private static String getGoldenFileResource(String fileName) {
    return "goldenfiles/expected/" + fileName + ".yaml";
  }
  private static String messageToFix(String fileName) {
    try {
      String actualPath = getGoldenFileActualPath(fileName).toString();
      String goldenPath = "src/test/resources/" + getGoldenFileResource(fileName);
      return ""
          + "To fix:\n"
          + "\t`cp " + actualPath+ " " + goldenPath + "`\n"
          + "To Diff:\n"
          + "\t`sdiff " + actualPath+ " " + goldenPath + "`\n";
    } catch (IOException exception) {
      return null;
    }
  }

  private static <I, O> void writeActualGoldenFile(
    Path goldenFileActualPath,
    List<InputAndOutput<I, O>> actualInputAndOutputList) throws IOException {
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
    String fileContentWithLicence = licenseHeaderContent + '\n' + fileContent;
    Files.write(goldenFileActualPath, fileContentWithLicence.getBytes(StandardCharsets.UTF_8));
  }

  private static <I, O> void assertGoldenFilesAreEqual(
    String fileName,
    List<InputAndOutput<I, O>> expectedInputAndOutputList,
    List<InputAndOutput<I, O>> actualInputAndOutputList) throws JsonProcessingException {
    String messageToFix = messageToFix(fileName);
    Assert.assertEquals(messageToFix, expectedInputAndOutputList.size(), actualInputAndOutputList.size());

    for (int i = 0; i < expectedInputAndOutputList.size(); i++) {
      InputAndOutput expectedInputAndOutput = expectedInputAndOutputList.get(i);
      InputAndOutput actualInputAndOutput = actualInputAndOutputList.get(i);

      Assert.assertEquals(
        "Descriptions differ,\n" + messageToFix,
        expectedInputAndOutput.description,
        actualInputAndOutput.description);
      String expectedInputString = objectMapper.writeValueAsString(expectedInputAndOutput.input);
      String actualInputString = objectMapper.writeValueAsString(actualInputAndOutput.input);
      Assert.assertEquals(
        "Inputs for baseline differ,\n" + messageToFix,
        expectedInputString,
        actualInputString);

      String expectedOutputString = objectMapper.writeValueAsString(expectedInputAndOutput.output);
      String actualOutputString = objectMapper.writeValueAsString(actualInputAndOutput.output);
      Assert.assertEquals(
        "Outputs for baselines differ,\n" + messageToFix + " with input " + expectedInputString,
        expectedOutputString,
        actualOutputString);

      Assert.assertEquals(
        "Exceptions for baselines differ,\n" + messageToFix+ " with input " + expectedInputString,
        expectedInputAndOutput.exceptionMessage,
        actualInputAndOutput.exceptionMessage);
    }
  }

  @FunctionalInterface
  public interface ThrowingFunction<T,R> {
    R apply(T t) throws Exception;
  }

  private static final class DescriptionAndInput<I> {
    public final String description;
    public final I input;

    private DescriptionAndInput(String description, I input) {
      assert description != null;
      assert input != null;

      this.description = description;
      this.input = input;
    }
  }

  public static final class InputAndOutput<I, O> {
    public final String description;
    public final I input;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final O output;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final String exceptionMessage;

    @JsonCreator
    private InputAndOutput(
      @JsonProperty("description") String description,
      @JsonProperty("input") I input,
      @JsonProperty("output") O output,
      @JsonProperty("exceptionMessage") String exceptionMessage) {
      this.description = description;
      this.input = input;
      this.output = output;
      this.exceptionMessage = exceptionMessage;
    }

    public static <I, O> InputAndOutput createSuccess(String description, I input, O output) {
      return new InputAndOutput(description, input, output, null);
    }

    public static <I, O> InputAndOutput createFailure(String description, I input, Throwable throwable, boolean showFullStackTrace) {
      String exceptionMessage;
      if (showFullStackTrace) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);

        exceptionMessage = sw.toString().replace("\t", "");
      } else {
        exceptionMessage = throwable.getMessage();

      }

      if(exceptionMessage == null) {
        exceptionMessage = throwable.toString();
      }

      return new InputAndOutput(description, input, null, exceptionMessage);
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

  private static ObjectMapper getObjectMapper(){
    ObjectMapper objectMapper = new ObjectMapper(
      new YAMLFactory()
        .disable(YAMLGenerator.Feature.SPLIT_LINES)
        .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
        .enable(YAMLGenerator.Feature.INDENT_ARRAYS))
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);

    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    objectMapper.registerModule(new JavaTimeModule());

    return objectMapper;
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
        while(iterator.hasNext()) {
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

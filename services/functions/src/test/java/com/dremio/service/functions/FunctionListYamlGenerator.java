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

import com.dremio.service.functions.model.Function;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Test;

/** Tests that generates the yaml file that gets consumed in production. */
public final class FunctionListYamlGenerator {
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper(
              new YAMLFactory()
                  .disable(YAMLGenerator.Feature.SPLIT_LINES)
                  .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
                  .enable(YAMLGenerator.Feature.INDENT_ARRAYS))
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .registerModule(new JavaTimeModule())
          .registerModule(new GuavaModule())
          .registerModule(new Jdk8Module());
  private static final Path LICENSE_HEADER_PATH =
      Paths.get(Resources.getResource("goldenfiles/header.txt").getPath());

  @Test
  public void run() throws IOException {
    Map<String, Function> map = new TreeMap<>();

    for (String functionName : FunctionListDictionary.getFunctionNames()) {
      map.put(
          functionName.toUpperCase(), FunctionListDictionary.tryGetFunction(functionName).get());
    }

    Path outputPath = Paths.get("target", "functions.yaml");

    try {
      Files.createFile(outputPath);
    } catch (FileAlreadyExistsException exception) {
      // Do Nothing.
    }

    OBJECT_MAPPER.writeValue(new File(outputPath.toUri().getPath()), map);

    // Prepend the license header
    String fileContent = new String(Files.readAllBytes(outputPath));
    String licenseHeaderContent = new String(Files.readAllBytes(LICENSE_HEADER_PATH));
    String fileContentWithLicence = licenseHeaderContent + '\n' + fileContent;
    Files.write(outputPath, fileContentWithLicence.getBytes(StandardCharsets.UTF_8));
  }
}

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
import com.dremio.service.functions.model.FunctionSignatureComparator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FilenameUtils;

/** Singleton Dictionary used to interface with the static resource files for function list API. */
public final class FunctionListDictionary {
  private static final ConcurrentMap<String, Object> FILE_SYSTEM_LOCKS = new ConcurrentHashMap<>();

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper(
              new YAMLFactory()
                  .disable(YAMLGenerator.Feature.SPLIT_LINES)
                  .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
                  .enable(YAMLGenerator.Feature.INDENT_ARRAYS))
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .registerModule(new JavaTimeModule())
          .registerModule(new GuavaModule())
          .registerModule(new Jdk8Module());

  private static final ImmutableMap<String, FunctionSpecWithInfo> MAP = getFunctions();

  private FunctionListDictionary() {}

  public static Set<String> getFunctionNames() {
    return MAP.keySet();
  }

  public static Set<String> getDocumentedFunctionNames() {
    return MAP.keySet().stream()
        .filter(key -> MAP.get(key).isDocumented)
        .collect(Collectors.toSet());
  }

  public static Optional<Function> tryGetFunction(String functionName) {
    functionName = functionName.toUpperCase();
    FunctionSpecWithInfo functionSpecWithInfo = MAP.get(functionName);
    if (functionSpecWithInfo == null) {
      return Optional.empty();
    }

    return Optional.of(functionSpecWithInfo.function);
  }

  private static ImmutableMap<String, FunctionSpecWithInfo> getFunctions() {
    Map<String, FunctionSpecWithInfo> documentedFunctions =
        getFunctions("function_specs/documented", true);
    Map<String, FunctionSpecWithInfo> undocumentedFunctions =
        getFunctions("function_specs/undocumented", false);
    Map<String, FunctionSpecWithInfo> allFunctions = new HashMap<>();
    for (String functionName : documentedFunctions.keySet()) {
      allFunctions.put(functionName.toUpperCase(), documentedFunctions.get(functionName));
    }

    for (String functionName : undocumentedFunctions.keySet()) {
      if (allFunctions.containsKey(functionName)) {
        throw new RuntimeException(
            "The following function is already documented: '" + functionName + "'");
      }

      allFunctions.put(functionName.toUpperCase(), undocumentedFunctions.get(functionName));
    }

    return ImmutableMap.copyOf(allFunctions);
  }

  private static Map<String, FunctionSpecWithInfo> getFunctions(
      String resourceName, boolean isDocumented) {
    Map<String, FunctionSpecWithInfo> map = new HashMap<>();
    for (Path path : getFunctionFilePaths(resourceName)) {
      try {
        String fileName =
            FilenameUtils.removeExtension(path.getFileName().toString()).toUpperCase();
        Function functionSpec =
            OBJECT_MAPPER.readValue(path.toUri().toURL(), new TypeReference<Function>() {});
        functionSpec = sortAndNormalizeFunction(functionSpec);
        FunctionSpecWithInfo withInfo = new FunctionSpecWithInfo(functionSpec, isDocumented);
        map.put(fileName, withInfo);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load function: " + path.getFileName(), e);
      }
    }

    return map;
  }

  private static List<Path> getFunctionFilePaths(String directoryPath) {
    URI uri;
    try {
      uri = Resources.getResource(directoryPath).toURI();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    if (!"jar".equals(uri.getScheme())) {
      return getFilesInDirectory(Paths.get(uri));
    }

    synchronized (getLock(uri)) {
      try (FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
        return getFilesInDirectory(fileSystem.getPath(directoryPath));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static List<Path> getFilesInDirectory(Path path) {
    try (Stream<Path> stream = Files.walk(path)) {
      return stream.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Object getLock(URI uri) {
    String schemeSpecificPart = uri.getSchemeSpecificPart();
    String fileName = schemeSpecificPart.substring(0, schemeSpecificPart.indexOf("!"));
    FILE_SYSTEM_LOCKS.computeIfAbsent(fileName, s -> new Object());
    return FILE_SYSTEM_LOCKS.get(fileName);
  }

  private static Function sortAndNormalizeFunction(Function function) {
    return Function.builder()
        .name(function.getName().toUpperCase())
        .addAllSignatures(
            function.getSignatures().stream()
                .sorted(FunctionSignatureComparator.INSTANCE)
                .collect(Collectors.toList()))
        .dremioVersion(function.getDremioVersion())
        .functionCategories(function.getFunctionCategories())
        .description(function.getDescription())
        .build();
  }

  private static final class FunctionSpecWithInfo {
    private final Function function;
    private final boolean isDocumented;

    private FunctionSpecWithInfo(Function function, boolean isDocumented) {
      this.function = function;
      this.isDocumented = isDocumented;
    }
  }
}

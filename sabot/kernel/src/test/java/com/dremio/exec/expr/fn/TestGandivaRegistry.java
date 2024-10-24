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
package com.dremio.exec.expr.fn;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.options.OptionManager;
import com.google.common.io.Resources;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.Mockito;

public class TestGandivaRegistry {

  @Test
  public void testFunctionList() throws Exception {
    String goldFileName = "gandivaFunctions.csv";

    // Look for gold list of functions.
    try (InputStream goldFunctions = Resources.getResource("gandivaFunctions.csv").openStream()) {
      // Generate function list and compare to gold list.
      Path currentFunctionsFile = Files.createTempFile("currentGandivaFunctions", "csv");
      writeFunctionListToFile(currentFunctionsFile);
      InputStream currentFunctionsStream =
          new FileInputStream(String.valueOf(currentFunctionsFile));
      assertTrue(
          "Current Gandiva functions do not match the gold list. Consider the performance and behavior implications of new or missing functions before updating the gold list. Current functions: "
              + currentFunctionsFile,
          IOUtils.contentEquals(goldFunctions, currentFunctionsStream));
      Files.delete(currentFunctionsFile);
    } catch (Exception e) {
      // Gold file doesn't exist, generate the existing function list.
      Path file = Files.createFile(Paths.get(goldFileName));
      writeFunctionListToFile(file);
      fail("Gold file not present.");
    }
  }

  private void writeFunctionListToFile(Path filePath) throws Exception {
    OptionManager mockOptionManager = Mockito.mock(OptionManager.class);
    GandivaFunctionRegistry gandivaFunctions = new GandivaFunctionRegistry(true, mockOptionManager);

    List<String> lines = new ArrayList<>();
    Map<String, List<AbstractFunctionHolder>> functions = gandivaFunctions.getSupportedFunctions();
    functions.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              List<AbstractFunctionHolder> functionVersions = entry.getValue();
              for (AbstractFunctionHolder afh : functionVersions) {
                String line = entry.getKey() + "," + afh.toString().replace(',', ' ');
                lines.add(line);
              }
            });

    Collections.sort(lines);
    Files.write(filePath, lines, StandardCharsets.UTF_8);
  }
}

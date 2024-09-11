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
package com.dremio.store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dremio.plugins.dataplane.store.AzureStorageIcebergFilePathSanitizer;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TestAzureStorageIcebergFilePathSanitizer {

  private final List<String> pathEndingWithDot = List.of("folder1.", "folder2.", "table1.");
  private final List<String> pathEndingWithSlash = List.of("folder1/", "folder2/", "table1/");

  @Test
  public void testFilePathSanitizerWithDot() {
    AzureStorageIcebergFilePathSanitizer pathSanitizer = new AzureStorageIcebergFilePathSanitizer();
    List<String> sanitizedPath = pathSanitizer.getPath(pathEndingWithDot);
    assertEquals("folder1%2E", sanitizedPath.get(0));
    assertEquals("folder2%2E", sanitizedPath.get(1));
    assertEquals("table1%2E", sanitizedPath.get(2));
  }

  @Test
  public void testFilePathSanitizerWithSlash() {
    AzureStorageIcebergFilePathSanitizer pathSanitizer = new AzureStorageIcebergFilePathSanitizer();
    List<String> sanitizedPath = pathSanitizer.getPath(pathEndingWithSlash);
    assertEquals("folder1%2F", sanitizedPath.get(0));
    assertEquals("folder2%2F", sanitizedPath.get(1));
    assertEquals("table1%2F", sanitizedPath.get(2));
  }
}

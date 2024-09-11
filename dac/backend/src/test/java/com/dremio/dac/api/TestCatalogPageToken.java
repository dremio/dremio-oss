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
package com.dremio.dac.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dremio.catalog.model.VersionContext;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestCatalogPageToken {
  @Test
  public void testToApiToken() {
    CatalogPageToken token =
        CatalogPageToken.builder()
            .setVersionContext(VersionContext.ofBranch("-"))
            .setPageToken("folder1")
            .setPath("source.test")
            .build();
    assertEquals(
        "eyJ2ZXJzaW9uQ29udGV4dCI6eyJ0eXBlIjoiQlJBTkNIIiwidmFsdWUiOiItIn0sInBhdGgiOiJzb3VyY2UudGVzdCIsInBhZ2VUb2tlbiI6ImZvbGRlcjEifQ",
        token.toApiToken());
  }

  @Test
  public void testFromApiToken() {
    CatalogPageToken token =
        CatalogPageToken.builder()
            .setVersionContext(VersionContext.ofBranch("-"))
            .setPageToken("folder1")
            .setPath("source.test")
            .build();
    assertEquals(token, CatalogPageToken.fromApiToken(token.toApiToken()));
  }

  private static Stream<Arguments> testPathConversionArguments() {
    return Stream.of(
        Arguments.of("a.b.c", ImmutableList.of("a", "b", "c")),
        Arguments.of("\"a.b\".c", ImmutableList.of("a.b", "c")),
        Arguments.of("\"a\"\"b\".c", ImmutableList.of("a\"b", "c")),
        Arguments.of("\"a.\"\"b\".c", ImmutableList.of("a.\"b", "c")));
  }

  @ParameterizedTest
  @MethodSource("testPathConversionArguments")
  public void testPathConversion(String escaped, List<String> components) {
    assertEquals(escaped, CatalogPageToken.pathToString(components));
    assertEquals(components, CatalogPageToken.stringToPath(escaped));
  }

  @Test
  public void testFromStartChild() {
    assertEquals(
        new ImmutableCatalogPageToken.Builder()
            .setPath("source.folder")
            .setPageToken("subfolder")
            .build(),
        CatalogPageToken.fromStartChild(ImmutableList.of("source", "folder", "subfolder")));
  }
}

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
package com.dremio.service.autocomplete.catalog;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.dremio.service.autocomplete.DremioToken;
import com.dremio.service.autocomplete.SqlQueryTokenizer;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Tests for CatalogNodeResolver.
 */
public final class CatalogNodeResolverTests {
  public static final CatalogNodeResolver CATALOG_NODE_RESOLVER = new CatalogNodeResolver(CatalogNodeReaderTests.MOCK_CATALOG_NODE_READER);

  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(CatalogNodeResolverTests::executeTest)
      .add(
        "FROM CLAUSE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM "))
      .add(
        "MID PATH",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\"."))
      .add(
        "PATH WITH MANY CHILDREN",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\"."))
      .add(
        "PATH WITH NO CHILDREN",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"file\"."))
      .add(
        "INVALID PATH",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"path\".\"that\".\"does\".\"not\".\"exist\"."))
      .add(
        "MULTIPLE TABLES",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical_dataset\", \"space\".\"folder\"."))
      .add(
        "JOIN empty path",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical_dataset\" JOIN "))
      .add(
        "JOIN mid path",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical_dataset\" JOIN \"space\".\"folder\"."))
      .add(
        "APPLY empty path",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical_dataset\" APPLY "))
      .add(
        "APPLY mid path",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical_dataset\" APPLY \"space\".\"folder\"."))
      .runTests();
  }

  private static List<String> executeTest(GoldenFileTestBuilder.MultiLineString query) {
    String corpus = query.toString();
    List<DremioToken> tokens = SqlQueryTokenizer.tokenize(corpus);

    return CATALOG_NODE_RESOLVER
      .resolve(ImmutableList.copyOf(tokens))
      .stream()
      .map(catalogNode -> catalogNode.getName())
      .collect(Collectors.toList());
  }
}

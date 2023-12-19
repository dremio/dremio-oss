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
package com.dremio.exec.catalog.dataplane;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.google.common.collect.Streams;

/**
 * Helper class for {@link InfoSchemaCombinationCases}.
 *
 * Represents a container (source or folder) that may contain tables, views, and
 * other folders. In the combination test, we are creating a similar set of
 * tables and views, so this class helps us be consistent about that creation.
 */
public class ContainerEntity {

  private static final Joiner DOT_JOINER = Joiner.on('.');
  private static final Joiner QUOTE_DOT_QUOTE_JOINER = Joiner.on("\".\"");
  public static final String tableAFirst = "tableAFirst";
  public static final String tableBSecond = "tableBSecond";
  public static final String viewCThird = "viewCThird";
  public static final String viewDFourth = "viewDFourth";

  public enum Type {
    SOURCE,
    IMPLICIT_FOLDER,
    EXPLICIT_FOLDER,
  }

  public enum Contains {
    FOLDERS_ONLY,
    TABLES_ONLY,
    FOLDERS_AND_VIEWS,
    FOLDERS_TABLES_AND_VIEWS,
    FOLDERS_AND_TABLES,
    MAX_KEY_TABLE,
    EMPTY,

  }

  private final String name;
  private final Type type;
  private final Contains contains;
  private final List<String> parentPath;

  public ContainerEntity(String name, Type type, Contains contains, List<String> parentPath) {
    this.name = name;
    this.type = type;
    this.contains = contains;
    this.parentPath = parentPath;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public Contains getContains() {
    return contains;
  }

  public List<String> getParentPath() {
    return parentPath;
  }

  public List<String> getFullPath() {
    return Streams.concat(getParentPath().stream(), Stream.of(getName())).collect(Collectors.toList());
  }

  public List<String> getPathWithoutRoot() {
    if (parentPath.size() == 0) {
      // If no parentPath, this IS the root so don't return it
      return Collections.emptyList();
    }
    return Streams.concat(getParentPath().stream().skip(1), Stream.of(getName())).collect(Collectors.toList());
  }

  public List<String> getChildPathWithoutRoot(String childName) {
    return Streams.concat(getPathWithoutRoot().stream(), Stream.of(childName)).collect(Collectors.toList());
  }

  public String asSqlIdentifier() {
    // Note: this won't work correctly when we add special characters
    return DOT_JOINER.join(getFullPath());
  }

  public List<List<String>> getExpectedTablesIncludingViews() {
    return Arrays.asList(
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        tableAFirst,
        "TABLE"),
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        tableBSecond,
        "TABLE"),
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        viewCThird,
        "VIEW"),
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        viewDFourth,
        "VIEW")
    );
  }

  public List<List<String>> getExpectedTablesWithoutViews() {
    return Arrays.asList(
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        tableAFirst,
        "TABLE"),
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        tableBSecond,
        "TABLE"));
  }

  public List<List<String>> getExpectedTablesForNonNessieContainers() {
    return getExpectedTablesWithoutViews();
  }

  public List<List<String>> getTableAOnly() {
    return Collections.singletonList(
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        tableAFirst,
        "TABLE"));
  }

  public List<List<String>> getTableBOnly() {
    return Collections.singletonList(
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        tableBSecond,
        "TABLE"));
  }

  public List<List<String>> getExpectedViews() {
    // Note: this won't work correctly when we add special characters
    return Arrays.asList(
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        viewCThird,
        String.format("SELECT * FROM %s.%s", DOT_JOINER.join(getFullPath()), tableAFirst)),
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        viewDFourth,
        String.format("SELECT * FROM %s.%s", DOT_JOINER.join(getFullPath()), tableBSecond))
    );
  }

  public List<List<String>> getExpectedViewsForSelect1Query() {
    return Arrays.asList(
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        viewCThird,
        "SELECT 1"),
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        viewDFourth,
        "SELECT 1")
    );
  }

  public List<List<String>> getViewCOnly() {
    // Note: this won't work correctly when we add special characters
    return Collections.singletonList(
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        viewCThird,
        String.format("SELECT * FROM %s.%s", DOT_JOINER.join(getFullPath()), tableAFirst)));
  }

  public List<List<String>> getViewDOnly() {
    // Note: this won't work correctly when we add special characters
    return Collections.singletonList(
      Arrays.asList(
        "DREMIO",
        DOT_JOINER.join(getFullPath()),
        viewDFourth,
        String.format("SELECT * FROM %s.%s", DOT_JOINER.join(getFullPath()), tableBSecond)));
  }

  public List<String> getExpectedSchemata() {
    return Arrays.asList(
      "DREMIO",
      DOT_JOINER.join(getFullPath()),
      "<owner>",
      "SIMPLE",
      "NO");
  }

  /**
   * Overriding toString allows us to name parameterized tests more clearly.
   */
  @Override
  public String toString() {
    return getName();
  }
}

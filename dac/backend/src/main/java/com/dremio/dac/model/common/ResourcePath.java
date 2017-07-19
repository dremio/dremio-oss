/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.model.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/**
 * A resource path
 * `/name1/{value1}/name2/{value2}/...
 */
public abstract class ResourcePath {

  private static final Joiner joiner = Joiner.on("/").useForNull("");
  private static final Splitter splitter = Splitter.on("/");

  @JsonCreator
  public static ResourcePath defaultImpl(final String pathString) {
    return new ResourcePath() {
      @Override
      public List<?> asPath() {
        return toPath(pathString);
      }
    };
  }

  protected static final List<String> toPath(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    List<String> result = new ArrayList<>();
    Iterable<String> split = splitter.split(path);
    for (String element : split) {
      result.add(element);
    }
    return result;
  }

  protected static List<String> parse(String path, String... pathNames) {
    List<String> pathElements = toPath(path);
    if (pathElements.size() != pathNames.length * 2) {
      throw new IllegalArgumentException("the sizes don't match : " + path + " and " + Arrays.toString(pathNames) + " => " + pathElements);
    }
    List<String> values = new ArrayList<>();
    for (int i = 0; i < pathNames.length; i++) {
      String pathName = pathNames[i];
      int j = i*2;
      String key = pathElements.get(j);
      String value = pathElements.get(j + 1);
      if (!pathName.equals(key)) {
        throw new IllegalArgumentException("the value in " + i + " th in " + path + " position should be " + pathName + " found " + key);
      }
      values.add(value);
    }
    return values;
  }

  public abstract List<?> asPath();

  @JsonValue
  @Override
  public String toString() {
    return "/" + joiner.join(asPath());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ResourcePath) {
      return asPath().equals(((ResourcePath)obj).asPath());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return asPath().hashCode();
  }
}

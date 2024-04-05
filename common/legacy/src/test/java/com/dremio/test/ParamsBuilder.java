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

import java.util.ArrayList;
import java.util.List;

/**
 * Builder class to help generating {@code Object[][]} for parameterized tests. No error handling is
 * implemented since JUnit will do it anyway.
 *
 * @see org.junit.runners.Parameterized.Parameters
 */
public class ParamsBuilder {

  private final List<List<Object>> vectors = new ArrayList<>();

  /** Creates a builder with the specified values as separate parameter vectors. */
  public static ParamsBuilder ofDimension(Object... values) {
    return new ParamsBuilder().addDimension(values);
  }

  /** Creates a builder with the specified parameters as the only one parameter vector. */
  public static ParamsBuilder ofVector(Object... params) {
    return new ParamsBuilder().addVector(params);
  }

  /**
   * Adds a new dimension to the existing parameter vectors by creating the Cartesian product of the
   * vectors with the specified values;
   */
  public ParamsBuilder addDimension(Object... values) {
    int origSize = vectors.size();
    if (origSize == 0) {
      for (Object value : values) {
        addVector(value);
      }
    }
    for (int i = 1; i < values.length; ++i) {
      Object value = values[i];
      for (int j = 0; j < origSize; ++j) {
        List<Object> newVector = new ArrayList<>(vectors.get(j));
        newVector.add(value);
        vectors.add(newVector);
      }
    }
    Object first = values[0];
    for (int i = 0; i < origSize; ++i) {
      vectors.get(i).add(first);
    }
    return this;
  }

  /** Adds a parameter vector to the existing ones. */
  public ParamsBuilder addVector(Object... params) {
    List<Object> vector = new ArrayList<>();
    for (Object param : params) {
      vector.add(param);
    }
    vectors.add(vector);
    return this;
  }

  /** Adds the current content of the specified builder to this builder */
  public ParamsBuilder addBuilder(ParamsBuilder builder) {
    vectors.addAll(builder.vectors);
    return this;
  }

  /** Builds the array of parameter arrays. */
  public Object[][] build() {
    Object[][] out = new Object[vectors.size()][];
    for (int i = 0; i < out.length; ++i) {
      List<Object> vector = vectors.get(i);
      out[i] = vector.toArray(new Object[vector.size()]);
    }
    return out;
  }
}

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
package com.dremio.service.autocomplete.tokens;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import org.apache.arrow.util.Preconditions;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * Factory for creating Markov Chains of SQL tokens trained on data sets from the resource folder.
 */
public final class ResourceBackedMarkovChainFactory {
  private ResourceBackedMarkovChainFactory() {}

  public static SqlTokenKindMarkovChain create(String resourcePath) {
    Preconditions.checkNotNull(resourcePath);

    final URL url = Resources.getResource(resourcePath);
    if (url == null) {
      throw new RuntimeException("file not found! " + resourcePath);
    }

    String[] queries = null;
    try {
      queries = Resources
        .toString(url, Charsets.UTF_8)
        .split(System.lineSeparator());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    SqlTokenKindMarkovChain markovChain = new SqlTokenKindMarkovChain();
    markovChain.addQueries(Arrays.stream(queries));

    return markovChain;
  }
}

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
package com.dremio.plugins.dataplane.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.parquet.SemanticVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSemanticVersion {

  private static final ListOrderedSet<String> orderedVersionsToCompare =
      ListOrderedSet.listOrderedSet(
          Arrays.asList(
              "1.0.0-alpha",
              "1.0.0-alpha.1",
              "1.0.0-alpha.beta",
              "1.0.0-beta",
              "1.0.0-beta.2",
              "1.0.0-beta.11",
              "1.0.0-rc.1",
              "1.0.0",
              "2.0.0",
              "2.1.0",
              "2.1.1"));

  private static Stream<Arguments> allPairsOfVersions() {
    return Sets.cartesianProduct(orderedVersionsToCompare, orderedVersionsToCompare).stream()
        .map(pair -> Arguments.of(pair.get(0), pair.get(1)));
  }

  @ParameterizedTest(name = "{index} {0} {1}")
  @MethodSource("allPairsOfVersions")
  public void testPrecedenceWithDifferentVersions(String string1, String string2) throws Exception {
    SemanticVersion semanticVersion1 = SemanticVersion.parse(string1);
    SemanticVersion semanticVersion2 = SemanticVersion.parse(string2);

    Integer order1 = orderedVersionsToCompare.indexOf(string1);
    Integer order2 = orderedVersionsToCompare.indexOf(string2);

    int semanticVersionOrder = Integer.signum(semanticVersion1.compareTo(semanticVersion2));
    int expectedOrder = Integer.signum(order1.compareTo(order2));

    assertThat(semanticVersionOrder).isEqualTo(expectedOrder);
  }

  @Test
  public void testPrecedenceWithDifferentMetadata() throws Exception {
    SemanticVersion a = SemanticVersion.parse("1.0.0-alpha+001");
    SemanticVersion b = SemanticVersion.parse("1.0.0-alpha+20130313144700");
    assertThat(a.compareTo(b)).isEqualTo(0);
  }
}

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
package com.dremio.exec.planner.sql.handlers.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.dremio.exec.proto.UserProtos;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.Lists;

/**
 * Tests different characters in filters
 */
public class TestMetadataProviderFromFilter {
  private static Stream<Arguments> testMetadataFilter() {
    return Stream.of(
      Arguments.of(
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.bcd_def.ghk:AMP.foo", "MY_TABLE_foobar"))
      ),
      Arguments.of(
        toFilter("abc.b\\\\cd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\%\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.b\\cd_def.ghk:AMP.foo", "MY_TABLE%_foobar"))
      ),
      Arguments.of(
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TA\\+BLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.bcd_def.ghk:AMP.foo", "MY_TA+BLE_foobar"))
      ),
      Arguments.of(
        toFilter("abc.b\\*cd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.b*cd_def.ghk:AMP.foo", "MY_TABLE_foobar"))
      ),
      Arguments.of(
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TA\\^BLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.bcd_def.ghk:AMP.foo", "MY_TA^BLE_foobar"))
      ),
      Arguments.of(
        toFilter("abc.b\\(\\)cd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.b()cd_def.ghk:AMP.foo", "MY_TABLE_foobar"))
      ),
      Arguments.of(
        toFilter("abc.b\\(\\)cd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\.foobar"),
        new NamespaceKey(Lists.newArrayList("abc.b()cd_def.ghk:AMP.foo", "MY_TABLE.foobar"))
      ),
      Arguments.of(
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter("%MY\\_TABLE\\_foobar"),
        null
      ),
      Arguments.of(
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter(""),
        null
      ),
      Arguments.of(
        toFilter(""),
        toFilter("MY\\_TABLE\\_foobar"),
        null
      )
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testMetadataFilter(UserProtos.LikeFilter schema, UserProtos.LikeFilter table, NamespaceKey
    spaceKey) throws Exception {
    assertEquals(spaceKey, MetadataProvider.getTableKeyFromFilter(schema, table));
  }

  private static UserProtos.LikeFilter toFilter(String pattern, String escape) {
    return UserProtos.LikeFilter.newBuilder().setEscape(escape).setPattern(pattern)
      .build();
  }

  private static UserProtos.LikeFilter toFilter(String pattern) {
    return toFilter(pattern, "\\");
  }
}

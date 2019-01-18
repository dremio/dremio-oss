/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static junit.framework.TestCase.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.dremio.exec.proto.UserProtos;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.Lists;

/**
 * Tests different characters in filters
 */
@RunWith(Parameterized.class)
public class TestMetadataProviderFromFilter {
  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      {
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.bcd_def.ghk:AMP.foo", "MY_TABLE_foobar"))
      },
      {
        toFilter("abc.b\\\\cd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\%\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.b\\cd_def.ghk:AMP.foo", "MY_TABLE%_foobar"))
      },
      {
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TA\\+BLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.bcd_def.ghk:AMP.foo", "MY_TA+BLE_foobar"))
      },
      {
        toFilter("abc.b\\*cd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.b*cd_def.ghk:AMP.foo", "MY_TABLE_foobar"))
      },
      {
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TA\\^BLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.bcd_def.ghk:AMP.foo", "MY_TA^BLE_foobar"))
      },
      {
        toFilter("abc.b\\(\\)cd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\_foobar"),
        new NamespaceKey(Lists.newArrayList("abc.b()cd_def.ghk:AMP.foo", "MY_TABLE_foobar"))
      },
      {
        toFilter("abc.b\\(\\)cd\\_def.ghk:AMP.foo"),
        toFilter("MY\\_TABLE\\.foobar"),
        new NamespaceKey(Lists.newArrayList("abc.b()cd_def.ghk:AMP.foo", "MY_TABLE.foobar"))
      },
      {
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter("%MY\\_TABLE\\_foobar"),
        null
      },
      {
        toFilter("abc.bcd\\_def.ghk:AMP.foo"),
        toFilter(""),
        null
      },
      {
        toFilter(""),
        toFilter("MY\\_TABLE\\_foobar"),
        null
      }
    });
  }

  private UserProtos.LikeFilter schema;
  private UserProtos.LikeFilter table;
  private NamespaceKey spaceKey;

  public TestMetadataProviderFromFilter(UserProtos.LikeFilter schema, UserProtos.LikeFilter table, NamespaceKey
    spaceKey) {
    this.schema = schema;
    this.table = table;
    this.spaceKey = spaceKey;
  }

  @Test
  public void testMetadataFilter() throws Exception {
    assertEquals(spaceKey, MetadataProvider.fromFilter(schema, table));
  }

  private static UserProtos.LikeFilter toFilter(String pattern, String escape) {
    return UserProtos.LikeFilter.newBuilder().setEscape(escape).setPattern(pattern)
      .build();
  }

  private static UserProtos.LikeFilter toFilter(String pattern) {
    return toFilter(pattern, "\\");
  }
}

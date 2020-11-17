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
package com.dremio.exec.util;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecTest;
import com.dremio.exec.proto.UserBitShared;

/**
 * Test functions in SchemaPath
 */
public class TestSchemaPath extends ExecTest {

  @Test
  public void testSimplePath() {
    SchemaPath simplePath = SchemaPath.getSimplePath("string_field");
    Assert.assertTrue(simplePath.toDotString().equalsIgnoreCase("string_field"));
  }

  @Test
  public void testComplexPath() {
    SchemaPath simplePath = SchemaPath.getCompoundPath("a", "b", "c");
    Assert.assertTrue(simplePath.toDotString().equalsIgnoreCase("a.b.c"));
  }

  @Test
  public void testListPath() {
    UserBitShared.NamePart.Builder cPart = UserBitShared.NamePart.newBuilder();
    cPart.setName("c");

    UserBitShared.NamePart.Builder bArrayPart = UserBitShared.NamePart.newBuilder();
    bArrayPart.setType(UserBitShared.NamePart.Type.ARRAY);
    bArrayPart.setChild(cPart);

    UserBitShared.NamePart.Builder bPart = UserBitShared.NamePart.newBuilder();
    bPart.setName("b");
    bPart.setChild(bArrayPart);

    UserBitShared.NamePart.Builder aPart = UserBitShared.NamePart.newBuilder();
    aPart.setName("a");
    aPart.setChild(bPart);

    SchemaPath simplePath = SchemaPath.create(aPart.build());
    Assert.assertTrue(simplePath.toDotString().equalsIgnoreCase("a.b.list.element.c"));
  }

}

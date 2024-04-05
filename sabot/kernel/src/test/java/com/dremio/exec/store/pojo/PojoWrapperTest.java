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
package com.dremio.exec.store.pojo;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class PojoWrapperTest {

  @Test
  void testGetAllFields() {
    PojoWrapper<TestChildClass> pojoWrapper = PojoWrapper.from(TestChildClass.class);

    List<String> fields =
        pojoWrapper.getFields().stream().map(Field::getName).collect(Collectors.toList());

    List<String> expectedFields = Arrays.asList("FIELD_1", "FIELD_2");
    assertEquals(expectedFields, fields);
  }

  private static class TestParentClass {
    private final String FIELD_1 = "field_1";
  }

  private static class TestChildClass extends TestParentClass {
    private final String FIELD_2 = "field_2";
    private static final String FIELD_3 = "field_3";
  }
}

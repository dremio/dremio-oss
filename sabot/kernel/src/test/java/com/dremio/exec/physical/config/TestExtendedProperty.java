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
package com.dremio.exec.physical.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TestExtendedProperty {

  @Test
  public void testSerializeAndDeserialize() {
    // Create a TestProperty object

    TestProperty testProperty = new TestProperty("test", 42);

    // Serialize the TestProperty object to JSON
    String json = ExtendedProperty.Util.serialize(testProperty);

    // Deserialize the JSON back to a TestProperty object
    TestProperty deserializedProperty = ExtendedProperty.Util.deserialize(json, TestProperty.class);

    // Assert that the deserialized property is equal to the original property
    assertThat(testProperty.getName()).isEqualTo(deserializedProperty.getName());
    assertThat(testProperty.getValue()).isEqualTo(deserializedProperty.getValue());
  }

  private static class TestProperty implements ExtendedProperty {
    private String name;
    private int value;

    public TestProperty() {}

    public TestProperty(String name, int value) {
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public int getValue() {
      return value;
    }
  }
}

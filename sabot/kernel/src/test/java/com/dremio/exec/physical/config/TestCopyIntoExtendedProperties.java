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
import static org.junit.Assert.assertFalse;

import io.protostuff.ByteString;
import java.util.Optional;
import org.junit.Test;

public class TestCopyIntoExtendedProperties {

  @Test
  public void testSetAndGetProperties() {
    // Create an instance of ExtendedProperties
    CopyIntoExtendedProperties properties = new CopyIntoExtendedProperties();

    // Create a TestProperty object
    TestProperty testProperty = new TestProperty("test", 42);

    // Set the TestProperty as an extended property
    properties.setProperty(
        CopyIntoExtendedProperties.PropertyKey.COPY_INTO_QUERY_PROPERTIES, testProperty);

    // Get the TestProperty back from the extended properties
    TestProperty retrievedProperty =
        properties.getProperty(
            CopyIntoExtendedProperties.PropertyKey.COPY_INTO_QUERY_PROPERTIES, TestProperty.class);

    // Assert that the retrieved TestProperty is equal to the original TestProperty
    assertThat(testProperty.getName()).isEqualTo(retrievedProperty.getName());
    assertThat(testProperty.getValue()).isEqualTo(retrievedProperty.getValue());
  }

  @Test
  public void testSerializeAndDeserializeByteString() {
    // Create an instance of ExtendedProperties
    CopyIntoExtendedProperties properties = new CopyIntoExtendedProperties();

    // Create a TestProperty object
    TestProperty testProperty = new TestProperty("test", 42);

    // Set the TestProperty as an extended property
    properties.setProperty(CopyIntoExtendedProperties.PropertyKey.QUERY_CONTEXT, testProperty);

    // Serialize the ExtendedProperties to a ByteString
    ByteString byteString = CopyIntoExtendedProperties.Util.getByteString(properties);

    // Deserialize the ByteString back to ExtendedProperties
    CopyIntoExtendedProperties deserializedProperties =
        CopyIntoExtendedProperties.Util.getProperties(byteString).get();

    // Get the TestProperty from the deserialized ExtendedProperties
    TestProperty retrievedProperty =
        deserializedProperties.getProperty(
            CopyIntoExtendedProperties.PropertyKey.QUERY_CONTEXT, TestProperty.class);

    // Assert that the retrieved TestProperty is equal to the original TestProperty
    assertThat(testProperty.getName()).isEqualTo(retrievedProperty.getName());
    assertThat(testProperty.getValue()).isEqualTo(retrievedProperty.getValue());
  }

  @Test
  public void testEmptyByteString() {
    // Create an empty ByteString
    ByteString emptyByteString = ByteString.EMPTY;

    // Deserialize the empty ByteString to CopyIntoExtendedProperties
    Optional<CopyIntoExtendedProperties> emptyProperties =
        CopyIntoExtendedProperties.Util.getProperties(emptyByteString);

    // Assert that the deserialized CopyIntoExtendedProperties is empty
    assertFalse(emptyProperties.isPresent());
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

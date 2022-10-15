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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

public class TestTemporaryEnvironment {
  private static final String preExistingKey = "PWD";

  private String generateRandomString() {
    final int leftLimit = 97; // letter 'a'
    final int rightLimit = 122; // letter 'z'
    final int stringLength = 10;
    final Random random = new Random();

    return random.ints(leftLimit, rightLimit + 1)
      .limit(stringLength)
      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
      .toString();
  }

  @Test
  public void testPreExisting() {
    // Arrange
    final String originalValue = System.getenv(preExistingKey);
    final String newValue = generateRandomString();

    final TemporaryEnvironment temporaryEnvironment = new TemporaryEnvironment();

    // Act
    temporaryEnvironment.setEnvironmentVariable(preExistingKey, newValue);
    final String valueAfterSet = System.getenv(preExistingKey);

    temporaryEnvironment.after();

    final String valueAfterCleanup = System.getenv(preExistingKey);

    // Assert
    assertEquals(newValue, valueAfterSet);
    assertEquals(originalValue, valueAfterCleanup);
  }

  @Test
  public void testNew() {
    // Arrange
    final String newKey = generateRandomString();
    final String originalValue = System.getenv(newKey);
    final String newValue = generateRandomString();

    final TemporaryEnvironment temporaryEnvironment = new TemporaryEnvironment();

    // Act
    temporaryEnvironment.setEnvironmentVariable(newKey, newValue);
    final String valueAfterSet = System.getenv(newKey);

    temporaryEnvironment.after();

    final String valueAfterCleanup = System.getenv(newKey);

    // Assert
    assertEquals(newValue, valueAfterSet);
    assertEquals(originalValue, valueAfterCleanup);
  }

  @Test
  public void testMultiple() {
    // Arrange
    final String preExistingKeyOriginalValue = System.getenv(preExistingKey);
    final String preExistingKeyNewValue = generateRandomString();
    final String newKey = generateRandomString();
    final String newKeyOriginalValue = System.getenv(newKey);
    final String newKeyNewValue = generateRandomString();

    final TemporaryEnvironment temporaryEnvironment = new TemporaryEnvironment();

    // Act
    temporaryEnvironment.setEnvironmentVariable(preExistingKey, preExistingKeyNewValue);
    final String preExistingKeyValueAfterSet = System.getenv(preExistingKey);
    temporaryEnvironment.setEnvironmentVariable(newKey, newKeyNewValue);
    final String newKeyValueAfterSet = System.getenv(newKey);

    temporaryEnvironment.after();

    final String preExistingKeyValueAfterCleanup = System.getenv(preExistingKey);
    final String newKeyValueAfterCleanup = System.getenv(newKey);

    // Assert
    assertEquals(preExistingKeyNewValue, preExistingKeyValueAfterSet);
    assertEquals(preExistingKeyOriginalValue, preExistingKeyValueAfterCleanup);

    assertEquals(newKeyNewValue, newKeyValueAfterSet);
    assertEquals(newKeyOriginalValue, newKeyValueAfterCleanup);
  }
}

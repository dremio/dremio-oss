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
package com.dremio.exec.store;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;

/**
 * Tests for the {@code StoragePluginUtils} utility methods.
 */
public class StoragePluginUtilsTest {

  /**
   * Test that an error message is appropriately formatted with the source name.
   */
  @Test
  public void testGenerateSourceErrorMessage() {
    final String sourceName = "test-source";
    final String errorMessage = "Failed to establish connection";
    Assert.assertEquals("Source 'test-source' returned error 'Failed to establish connection'",
      StoragePluginUtils.generateSourceErrorMessage(sourceName, errorMessage));
  }

  @Test
  public void testGenerateSourceErrorMessageFromFormatString() {
    final String sourceName = "test-source";
    final String errorFmtString = "Returned status code %s from cluster";
    Assert.assertEquals("Source 'test-source' returned error 'Returned status code 500 from cluster'",
      StoragePluginUtils.generateSourceErrorMessage(sourceName, errorFmtString, "500"));
  }

  @Test
  public void testAddContextAndErrorMessageToUserException() {
    final UserException.Builder builder = UserException.validationError();
    final String errorMessageFormatString = "Invalid username: %s";
    final String sourceName = "fictitious-source";
    final UserException userException = StoragePluginUtils.message(
      builder, sourceName, errorMessageFormatString, "invalid-user").buildSilently();
    Assert.assertEquals("Source 'fictitious-source' returned error 'Invalid username: invalid-user'",
      userException.getMessage());
    Assert.assertEquals("plugin fictitious-source", userException.getContextStrings().get(0));
  }
}

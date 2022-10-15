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
package com.dremio.services.credentials;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for lookup in Data Credential Provider.
 */
@RunWith(Parameterized.class)
public class TestDataCredentialsProvider {
  /**
   * Type of test scenarios
   */
  private enum TEST_TYPE {
    VALID,     // valid data url for data credential provider
    INCORRECT, // incorrect data url representation in data credential provider
    INVALID    // invalid data url data credential provider, IllegalArgument expected
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      {TEST_TYPE.VALID, "Hello, World!", "data:,Hello%2C%20World!"},
      {TEST_TYPE.VALID, "Hello, World!", "data:text/plain;base64,SGVsbG8sIFdvcmxkIQ=="},
      {TEST_TYPE.VALID, "Hello, World!", "data:text/html;base64,SGVsbG8sIFdvcmxkIQ=="},
      {TEST_TYPE.INCORRECT, "Hello, World!", "data:text/html,SGVsbG8sIFdvcmxkIQ=="},
      {TEST_TYPE.INCORRECT, "Hello, World!", "data:,,Hello%2C%20World!"},
      {TEST_TYPE.INVALID, "Hello, World!", "data:abcdef"},
      {TEST_TYPE.INVALID, "Hello, World!", "data:text/plain;base644,SGVsbG8sIFdvcmxkIQ=="},
      {TEST_TYPE.INVALID, "Hello, World!", "data:text/plain;base16,SGVsbG8sIFdvcmxkIQ=="}
    });
  }

  @Parameter
  public TEST_TYPE type;

  @Parameter(1)
  public String originalString;

  @Parameter(2)
  public String pattern;

  @Test
  public void testDataLookupValid() throws CredentialsException {
    CredentialsProvider provider = new DataCredentialsProvider();
    URI uri = URI.create(pattern);
    switch (type) {
      case VALID:
        assertEquals(originalString, provider.lookup(uri));
        break;
      case INCORRECT:
        assertNotEquals("Return secret is not expected.", originalString, provider.lookup(uri));
        break;
      case INVALID:
        assertThatThrownBy(() -> provider.lookup(uri)).isInstanceOf(IllegalArgumentException.class);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported Test type " + type.toString());
    }
  }

}

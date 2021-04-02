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
package com.dremio.util;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;

import org.junit.Test;

import com.dremio.dac.server.tokens.TokenUtils;

/**
 * Tests for {@link TokenUtils}.
 */
public class TestTokenUtils {
  @Test
  public void testGetBearerTokenFromAuthHeader() throws Exception {
    assertEquals("a1.23Z.-_", TokenUtils.getBearerTokenFromAuthHeader("Bearer a1.23Z.-_"));
    assertEquals("a1.23Z.-_", TokenUtils.getBearerTokenFromAuthHeader("bearer a1.23Z.-_"));
    assertEquals("a1.23Z.-_", TokenUtils.getBearerTokenFromAuthHeader("bEaReR a1.23Z.-_"));
  }

  @Test
  public void testBearerTokenContainMultipleSpaces() throws Exception {
    // At least one space between prefix and token. (doc: https://tools.ietf.org/html/rfc6750#section-2.1)
    assertEquals("a1.23Z.-_", TokenUtils.getBearerTokenFromAuthHeader("Bearer   a1.23Z.-_"));
  }

  @Test(expected = ParseException.class)
  public void testBearerTokenWithInvalidPrefix() throws Exception {
    // wrong prefix
    TokenUtils.getBearerTokenFromAuthHeader("InvalidBearer a1.23Z.-_");
  }

  @Test(expected = ParseException.class)
  public void testBearerTokenContainInvalidCharacters() throws Exception {
    TokenUtils.getBearerTokenFromAuthHeader("Bearer 123 abcd");
  }
}

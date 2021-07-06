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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.dremio.test.DremioTest;

/**
 * Tests for Credentials Service Utils class:
 * 1) safeURICreate: throws IAE without original string
 */
public class TestCredentialsServiceUtils extends DremioTest {

  @Test
  public void testURICreate() {
    final String uriString = "SecretString%.isNotAValidURI";
    try {
      URI uri = URI.create(uriString);
      fail("Should have thrown someException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(uriString));
    }
  }

  @Test
  public void testSafeURICreate() {
    final String uriString = "SecretString%.isNotAValidURI";
    try {
      URI uri = CredentialsServiceUtils.safeURICreate(uriString);
      fail("Should have thrown someException");
    } catch (IllegalArgumentException e) {
      assertFalse(e.getMessage().contains(uriString));
    }
  }

  @Test
  public void testURICreateVsSafeURICreate() {
    Set<String> stringSet = new HashSet<>(Arrays.asList(
      "data:,Hello%2C%20World!",
      "data:text/html;base64,validStringForURI",
      "env:ldap",
      "file:///tmp/test/file",
      "secret:1.NotEncryptedContext.SampleOnly",
      "SecretString.isAValidURI"));
    for (String uriString:stringSet) {
      URI uri1 = URI.create(uriString);
      URI uri2 = CredentialsServiceUtils.safeURICreate(uriString);
      assertEquals(uri1, uri2);
      assertEquals(uri2, uri1);
    }
  }

}

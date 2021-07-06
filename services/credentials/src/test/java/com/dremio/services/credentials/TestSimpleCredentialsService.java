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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.test.DremioTest;


/**
 * Tests for:
 * 1) Lookup in Data Credential Provider through service.
 * 2) Lookup for legacy pattern (raw data) through service.
 */
public class TestSimpleCredentialsService extends DremioTest {
  private CredentialsService credentialsService;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    credentialsService = CredentialsService.newInstance(DEFAULT_DREMIO_CONFIG, CLASSPATH_SCAN_RESULT);
  }

  @Test
  public void testServiceDataLookup() throws Exception {

    String originalString = "Hello, World!";
    String basePattern = "data:,Hello%2C%20World!";
    String base64Pattern = "data:text/plain;base64,SGVsbG8sIFdvcmxkIQ==";
    String example1 = "data:";

    String ldap = credentialsService.lookup(basePattern);
    assertEquals(originalString, ldap);

    ldap = credentialsService.lookup(base64Pattern);
    assertEquals(originalString, ldap);

    thrown.expect(IllegalArgumentException.class);
    ldap = credentialsService.lookup(example1);

  }

  @Test
  public void testServiceLegacyLookup() throws Exception {
    String originalString = "SGVsbG8sIFdvcmxkIQ==";

    // emulate bindPassword like ad.json
    final String bindPassword = originalString;

    String ldap = credentialsService.lookup(bindPassword);
    assertEquals(originalString, ldap);

  }

  @Test
  public void testServiceLookupWithSpecialCharacter() throws Exception {
    String originalString = "SecretString%.isNotAValidURI";

    // emulate bindPassword like ad.json
    final String bindPassword = originalString;

    thrown.expect(IllegalArgumentException.class);
    String ldap = credentialsService.lookup(bindPassword);

  }

}

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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dremio.test.DremioTest;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for Credentials Service Utils class: 1) safeURICreate: throws IAE without original string
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
  public void testURICreate2() {
    final String uriString = "1234Secret+Str/ing==";
    URI uri = URI.create(uriString);
    assertNull(uri.getScheme());
    assertEquals(uriString, uri.toString());
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
    Set<String> stringSet =
        new HashSet<>(
            Arrays.asList(
                "data:,Hello%2C%20World!",
                "data:text/html;base64,validStringForURI",
                "env:ldap",
                "file:///tmp/test/file",
                "secret:1.NotEncryptedContext.SampleOnly",
                "SecretString.isAValidURI"));
    for (String uriString : stringSet) {
      URI uri1 = URI.create(uriString);
      URI uri2 = CredentialsServiceUtils.safeURICreate(uriString);
      assertEquals(uri1, uri2);
      assertEquals(uri2, uri1);
    }
  }

  @Test
  public void testIsEncryptedScheme() throws Exception {
    assertTrue(CredentialsServiceUtils.isEncryptedCredentials("system:secret"));
    assertTrue(CredentialsServiceUtils.isEncryptedCredentials(new URI("system:secret")));
    assertTrue(CredentialsServiceUtils.isEncryptedCredentials("secret:secret"));
    assertTrue(CredentialsServiceUtils.isEncryptedCredentials(new URI("secret:secret")));

    assertFalse(CredentialsServiceUtils.isEncryptedCredentials("data:,secret"));
    assertFalse(CredentialsServiceUtils.isEncryptedCredentials(new URI("data:,secret")));
  }

  @Test
  public void testIsDataCredentials() throws Exception {
    assertTrue(CredentialsServiceUtils.isDataCredentials(new URI("data:,secret")));
    assertFalse(CredentialsServiceUtils.isDataCredentials(new URI("system:secret")));
  }

  private static String[] getSampleSecrets() {
    return new String[] {
      // Sample generated secrets
      "vnzhlKJaERIb++THVJRsN7yA6qIKBmGWIX/hMlxOKz4bL/IYoMTyIhbDQcKqez0pzJ/eMFgHahwm+ASt0N90rw==",
      "Gf1KYOrjNLyBwsacmM7iGa1Mw0VOSFX01k8legvG9SsMaJnhshsrhYJDMvq8dDET5WqiOsb/LTwi+AStvUDHkg==",
      "KliNbKJHBLxSUpYSb6z8IkutzZ4C/HsmrP0/2rjvFat4aOS7FhKg98T35Aix2COezhT/8Kktj0Gx+AStBEJ1Kw==",
      "3dwc0I11eud2vGbnnEEIG20WgHyoF1GQwcO2dyB/qUcnY4zVIOfuUGd7ofIgtVA71f+bfcJ/dCGu+AStH/kIrA==",
      "LhocodlQy3KHD4TG0qvNa0uyEQlxVsYcFi0WabTzHCtE4QN/SwPbbceQ7KUz2YsdwxpML48+hPz5+ASt6/Xhpg==",
      // URI-like secrets
      "schema:header+vnzhlKJaERIb++THVJRsN7yA6qIKBmGWIX/hMlxOKz4bL/IYoMTyIhbDQcKqez0pzJ/eMFgHahwm+ASt0N90rw==",
      "schema:header+Gf1KYOrjNLyBwsacmM7iGa1Mw0VOSFX01k8legvG9SsMaJnhshsrhYJDMvq8dDET5WqiOsb/LTwi+AStvUDHkg==",
      "schema:header+KliNbKJHBLxSUpYSb6z8IkutzZ4C/HsmrP0/2rjvFat4aOS7FhKg98T35Aix2COezhT/8Kktj0Gx+AStBEJ1Kw==",
      "schema:header+3dwc0I11eud2vGbnnEEIG20WgHyoF1GQwcO2dyB/qUcnY4zVIOfuUGd7ofIgtVA71f+bfcJ/dCGu+AStH/kIrA==",
      "schema:header+LhocodlQy3KHD4TG0qvNa0uyEQlxVsYcFi0WabTzHCtE4QN/SwPbbceQ7KUz2YsdwxpML48+hPz5+ASt6/Xhpg==",
      // Synthetic secrets
      "~~~~", // produces "+" when base64 encoded
      "????", // produces "/" when base64 encoded
      "????~~~~" // produces both "+" and "/"
    };
  }

  /** Test encode on secrets */
  @ParameterizedTest
  @MethodSource("getSampleSecrets")
  public void testEncode(String secret) throws Exception {
    final URI dataURI = CredentialsServiceUtils.encodeAsDataURI(secret);
    // Check header
    assertTrue(dataURI.toString().startsWith("data:text/plain;base64,"));
    // Extract encoded segment
    final String encoded = dataURI.getSchemeSpecificPart().split(",", 2)[1];
    // Check value was encoded correctly
    assertEquals(secret, new String(Base64.getDecoder().decode(encoded)));
  }

  /** Test decode on secrets encoded with basic Base64 Encoder */
  @ParameterizedTest
  @MethodSource("getSampleSecrets")
  public void testDecodeBase64(String secret) throws Exception {
    final String encoded =
        Base64.getEncoder().encodeToString(secret.getBytes(StandardCharsets.UTF_8));
    final URI dataURI = new URI("data", "text/plain;base64," + encoded, null);
    assertEquals(secret, CredentialsServiceUtils.decodeDataURI(dataURI));
  }

  /** Test decode on secrets encoded with URL and Filename safe Base64 Encoder */
  @ParameterizedTest
  @MethodSource("getSampleSecrets")
  public void testDecodeBase64URL(String secret) throws Exception {
    final String encoded =
        Base64.getUrlEncoder().encodeToString(secret.getBytes(StandardCharsets.UTF_8));
    final URI dataURI = new URI("data", "text/plain;base64," + encoded, null);
    assertEquals(secret, CredentialsServiceUtils.decodeDataURI(dataURI));
  }

  /**
   * Attempt to decode an invalid encoded value. Base64 encoded strings may contain "+" and "/",
   * while Base64 URL encoded use "-" and "_" instead. Encoded values should not both, so we should
   * throw IllegalArgumentException
   */
  @Test
  public void testDecodeInvalid() throws Exception {
    final String encoded = "+/-_";
    final URI dataURI = new URI("data", "text/plain;base64," + encoded, null);
    assertThrows(
        IllegalArgumentException.class, () -> CredentialsServiceUtils.decodeDataURI(dataURI));
  }
}

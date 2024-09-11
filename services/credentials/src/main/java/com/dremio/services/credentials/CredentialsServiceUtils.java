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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Utility methods for credentials service. */
@Options
public final class CredentialsServiceUtils {

  public static final TypeValidators.BooleanValidator REMOTE_LOOKUP_ENABLED =
      new TypeValidators.BooleanValidator("services.credentials.exec.remote_lookup.enabled", false);

  /**
   * Create a URI from a String. If the String is not a valid URI, the exception thrown will not
   * contain the original string.
   *
   * @param pattern the string to create URI
   * @return URI created
   * @throws IllegalArgumentException
   */
  public static URI safeURICreate(String pattern) {
    try {
      return URI.create(pattern);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("The provided string is not a valid URI.");
    }
  }

  /** Check if remote lookup is enabled */
  public static boolean isRemoteLookupEnabled(OptionManager optionManager) {
    Preconditions.checkNotNull(optionManager);
    return optionManager.getOption(CredentialsServiceUtils.REMOTE_LOOKUP_ENABLED);
  }

  /**
   * Check if string matches the pattern of encrypted credentials. This does not validate if the
   * secret was actually encrypted by the system, only that it matches the pattern.
   */
  public static boolean isEncryptedCredentials(String pattern) {
    try {
      final URI uri = safeURICreate(pattern);
      return isEncryptedCredentials(uri);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /** Overload for URI formats, see other for description. */
  public static boolean isEncryptedCredentials(URI uri) {
    return SystemSecretCredentialsProvider.isSystemEncrypted(uri)
        || LocalSecretCredentialsProvider.isLocalEncrypted(uri);
  }

  /** Check if URI matches the pattern of data credentials. */
  public static boolean isDataCredentials(URI uri) {
    return "data".equalsIgnoreCase(uri.getScheme());
  }

  /** Decode data encoded URI. */
  public static String decodeDataURI(URI uri) throws IllegalArgumentException {
    if (!isDataCredentials(uri)) {
      throw new IllegalArgumentException("URI is not a Data URI");
    }
    final String dataUrl = uri.getSchemeSpecificPart();
    String[] parts = dataUrl.split(",", 2);
    if (parts.length == 1) {
      // ',' not found
      throw new IllegalArgumentException("Illegal Data URL.");
    }
    final String data = parts[1];
    final int baseIndex = parts[0].indexOf(';');
    if (baseIndex < 0) {
      // no ";" encoding, use data as-is
      return data;
    }
    if (!parts[0].substring(baseIndex + 1).equals("base64")) {
      throw new IllegalArgumentException("Decode data URI secret encounters error.");
    }

    // First, try to decode using basic Base 64 Alphabet
    try {
      return new String(Base64.getDecoder().decode(data), UTF_8);
    } catch (IllegalArgumentException ignored) {
      // fall through
    }
    // Try to decode using URL and Filename safe Base 64 Alphabet
    return new String(java.util.Base64.getUrlDecoder().decode(data), UTF_8);
  }

  /** Encode a UTF-8 string into base64 encoded Data URL with plain text content type. */
  public static URI encodeAsDataURI(String secret) throws CredentialsException {
    final String encoded =
        Base64.getEncoder().encodeToString(secret.getBytes(StandardCharsets.UTF_8));
    try {
      return new URI("data", "text/plain;base64," + encoded, null);
    } catch (URISyntaxException e) {
      throw new CredentialsException("Cannot encode secret into an URI");
    }
  }

  // prevent instantiation
  private CredentialsServiceUtils() {}
}

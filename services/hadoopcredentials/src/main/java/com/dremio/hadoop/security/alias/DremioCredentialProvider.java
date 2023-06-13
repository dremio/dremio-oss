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
package com.dremio.hadoop.security.alias;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.services.credentials.CredentialsException;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.CredentialsServiceUtils;
import com.google.common.base.Strings;

/**
 * A Dremio provider for credentials such as secret vault URIs, encrypted passwords,
 * passwords in plain text (not recommended) set in core-site.xml, which will be
 * resolved by the credentials providers accessible by Dremio.
 */
public class DremioCredentialProvider extends CredentialProvider {

  public static final String DREMIO_SCHEME_PREFIX = "dremio+";
  public static final String PROTOCOL_PREFIX = "https://";

  private static final Logger logger = LoggerFactory.getLogger(DremioCredentialProvider.class);

  private final CredentialsService credentialsService;
  private final Configuration conf;

  public DremioCredentialProvider(CredentialsService credentialsService, Configuration conf) {
    this.credentialsService = credentialsService;
    this.conf = conf;
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException("Flush is not available in Dremio Credential Provider.");
  }

   /**
   * @param alias the name of a specific credential
   * @return a pair of the credential name and its resolved secret.
   * @throws IOException if secret resolution fails.
   */
  @Override
  public CredentialEntry getCredentialEntry(String alias) throws IOException {
    final String pattern = conf.get(alias);
    if (pattern == null) {
      return null;
    }

    // Trim prefix "dremio+". If the prefix does not exist, return null. This implies the input is invalid here.
    if (!pattern.toLowerCase(Locale.ROOT).startsWith(DREMIO_SCHEME_PREFIX)) {
      return null;
    }
    final String trimmedPattern = pattern.substring(DREMIO_SCHEME_PREFIX.length());

    // Convert the input to a URI.
    final URI secretUri;
    try {
      secretUri = CredentialsServiceUtils.safeURICreate(pattern);
    } catch (IllegalArgumentException e) {
      // If it's not a URI, return the `pattern` as it is without "dremio+".
      return new DremioCredentialEntry(alias, trimmedPattern.toCharArray());
    }

    // Check if there is a scheme.
    // If scheme does not exist, return the `pattern` as it is without "dremio+".
    // If scheme exists, continue to Dremio credentials service.
    final String scheme = secretUri.getScheme();
    if (Strings.isNullOrEmpty(scheme)) {
      return new DremioCredentialEntry(alias, trimmedPattern.toCharArray());
    }
    final char[] secretBytes;
    try {
      String secret = credentialsService.lookup(trimmedPattern);
      secretBytes = secret.toCharArray();
      secret = null;
      return new DremioCredentialEntry(alias, secretBytes);
    } catch (IllegalArgumentException e) {
      logger.error("Failed to resolve {}", alias);
      throw new IOException(String.format("Failed to resolve '%s'", alias), e);
    } catch (CredentialsException e) {
      logger.error("Failed to resolve {}", alias);
      throw new IOException("Failed to resolve credentials: " + e.getMessage(), e);
    }
  }

  @Override
  public List<String> getAliases() {
    return Collections.emptyList();
  }

  @Override
  public CredentialEntry createCredentialEntry(String name, char[] credential) throws IOException {
    throw new UnsupportedOperationException("Credential creation is not available in Dremio Credential Provider.");
  }

  @Override
  public void deleteCredentialEntry(String name) throws IOException {
    throw new UnsupportedOperationException("Credential deletion is not available in Dremio Credential Provider.");
  }

  /**
   * Dremio representation of a resolved credential entry, containing the name
   * of the key and the resolved secret in byte array.
   */
  public static class DremioCredentialEntry extends CredentialEntry {
    protected DremioCredentialEntry(String alias, char[] credential) {
      super(alias, credential);
    }
  }
}

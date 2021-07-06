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

import java.net.URI;

import com.google.inject.Inject;

/**
 * Data Credential Provider.
 */
public class DataCredentialsProvider extends AbstractSimpleCredentialsProvider implements CredentialsProvider {

  @Inject
  public DataCredentialsProvider() {
    super("data");
  }

  @Override
  protected String doLookup(URI uri) throws IllegalArgumentException {

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
    return new String(java.util.Base64.getUrlDecoder().decode(data), UTF_8);

  }

}

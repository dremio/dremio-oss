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
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for Lookup in File Credential Provider. */
public class TestFileCredentialsProvider {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testProviderFileLookup() throws Exception {

    String originalString = "ldapPasswordCipherValidation";
    String dksLoc = "/test.file";

    String fileLoc = tempFolder.newFolder().getAbsolutePath().concat(dksLoc);

    // create the password file
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileLoc))) {
      writer.write(originalString);
    }

    final String bindPassword = "file://".concat(fileLoc);

    URI uri = URI.create(bindPassword);

    FileCredentialsProvider provider = new FileCredentialsProvider();

    boolean supported = provider.isSupported(uri);
    assertTrue(supported);

    String ldap = provider.lookup(uri);
    assertEquals(originalString, ldap);
  }
}

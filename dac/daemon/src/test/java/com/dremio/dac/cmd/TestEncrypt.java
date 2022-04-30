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
package com.dremio.dac.cmd;

import static com.dremio.config.DremioConfig.CREDENTIALS_KEYSTORE_PASSWORD;
import static com.dremio.config.DremioConfig.LOCAL_WRITE_PATH_STRING;
import static org.junit.Assert.assertEquals;

import java.net.URI;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.config.DremioConfig;
import com.dremio.test.DremioTest;

/**
 * Tests for Encrypt command
 */
public class TestEncrypt extends DremioTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testEncryptWithDefaultPassword() throws Exception {
    String secret = "secretEncryptDecryptValidation";
    DremioConfig conf = DEFAULT_DREMIO_CONFIG
        .withValue(LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().getAbsolutePath())
        .withValue(CREDENTIALS_KEYSTORE_PASSWORD, "dremio123");

    String uri = Encrypt.encrypt(conf, CLASSPATH_SCAN_RESULT, secret);
    assertEquals("secret", URI.create(uri).getScheme());

  }

  @Test
  public void testEncryptWithDataPasswordUri() throws Exception {

    String secret = "secretEncryptDecryptValidation";
    DremioConfig conf = DEFAULT_DREMIO_CONFIG
        .withValue(LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().getAbsolutePath())
        .withValue(CREDENTIALS_KEYSTORE_PASSWORD, "data:,dremio345");

    String uri = Encrypt.encrypt(conf, CLASSPATH_SCAN_RESULT, secret);
    assertEquals("secret", URI.create(uri).getScheme());

  }

}

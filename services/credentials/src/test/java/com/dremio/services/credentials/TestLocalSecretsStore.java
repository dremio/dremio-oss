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
import static org.junit.Assert.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.NoSuchElementException;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.config.DremioConfig;
import com.dremio.test.DremioTest;

/**
 * Tests for:
 * 1) basic encryption and decryption.
 * 2) dual KEK and DEK encryption and decryption.
 */

public class TestLocalSecretsStore extends DremioTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testEncryptDecrypt() throws Exception {

    String pText = "Use AES-GCM for encryption and decryption!";

    DremioConfig config = DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    LocalSecretsStore localSecretsStore = new LocalSecretsStore(config, null);
    // get AES 256 bits (32 bytes) key
    SecretKey secretKey = LocalSecretsStore.newAESKey();

    byte[] encryptedText = localSecretsStore.encrypt(pText.getBytes(StandardCharsets.UTF_8), secretKey);
    String decryptedText = localSecretsStore.decrypt(encryptedText, secretKey);

    assertEquals(pText, decryptedText);
  }

  @Test
  public void testEncryptDecryptWithDekKek() throws Exception {

    String plainText = "Use AES-GCM, and cipher and decipher the plainText!";

    DremioConfig config = DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    LocalSecretsStore localSecretsStore = new LocalSecretsStore(config, null);
    // create DEK and IV to encrypt plainText
    SecretKey dek = LocalSecretsStore.newAESKey();

    // encrypt the pText
    byte[] encryptedText = localSecretsStore.encrypt(plainText.getBytes(StandardCharsets.UTF_8), dek);

    // create KEK and IV to encrypt DEK
    SecretKey kek = LocalSecretsStore.newAESKey();

    // encrypt DEK
    String sdek = Base64.getEncoder().encodeToString(dek.getEncoded());
    byte[] encryptedDEK = localSecretsStore.encrypt(sdek.getBytes(StandardCharsets.UTF_8), kek);

    // decrypt DEK through KEK
    String sdecryptedDek = localSecretsStore.decrypt(encryptedDEK, kek);
    // decode the base64 encoded string
    byte[] decodedKey = Base64.getDecoder().decode(sdecryptedDek);
    // rebuild DEK using SecretKeySpec
    SecretKey originalKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");

    // decrypt plainText
    String decryptedText = localSecretsStore.decrypt(encryptedText, originalKey);

    assertEquals(plainText, decryptedText);
  }

  @Test(expected = CredentialsException.class)
  public void testInvalidToken() throws Exception {

    String pText = "Use AES-GCM for encryption and decryption!";

    DremioConfig config = DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    LocalSecretsStore localSecretsStore = new LocalSecretsStore(config, null);
    // get AES 256 bits (32 bytes) key
    SecretKey secretKey = LocalSecretsStore.newAESKey();

    byte[] encryptedText = localSecretsStore.encrypt(pText.getBytes(StandardCharsets.UTF_8), secretKey);
    String decryptedText = localSecretsStore.decrypt(Arrays.copyOfRange(encryptedText, 0, encryptedText.length-2), secretKey);
  }

  @Test(expected = CredentialsException.class)
  public void testUnderflowToken() throws Exception {

    String pText = "Use AES-GCM for encryption and decryption!";

    DremioConfig config = DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    LocalSecretsStore localSecretsStore = new LocalSecretsStore(config, null);
    // get AES 256 bits (32 bytes) key
    SecretKey secretKey = LocalSecretsStore.newAESKey();

    byte[] encryptedText = localSecretsStore.encrypt(pText.getBytes(StandardCharsets.UTF_8), secretKey);
    String decryptedText = localSecretsStore.decrypt(Arrays.copyOfRange(encryptedText, 0, 20), secretKey);
  }

  @Test
  public void testInvalidAlias() throws Exception {

    String pText = "Use AES-GCM for encryption and decryption!";

    DremioConfig config = DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    LocalSecretsStore localSecretsStore = new LocalSecretsStore(config, null);
    SecretKey key = localSecretsStore.lookupKeystore("master", true);
    assertNotNull(key);
    key = localSecretsStore.lookupKeystore("master", false);
    assertNotNull(key);

    thrown.expect(NoSuchElementException.class);
    key = localSecretsStore.lookupKeystore("invalidAlias", false);
  }

}

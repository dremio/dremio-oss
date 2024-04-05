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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.dremio.config.DremioConfig;
import com.dremio.test.DremioTest;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.NoSuchElementException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for: 1) basic encryption and decryption. 2) dual KEK and DEK encryption and decryption. */
public abstract class TestAbstractCipher extends DremioTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  protected abstract AbstractCipher getCipher(DremioConfig config);

  @Test
  public void testEncryptDecrypt() throws Exception {

    String pText = "Use AES-GCM for encryption and decryption!";

    DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(
            DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    AbstractCipher cipher = getCipher(config);
    // get AES 256 bits (32 bytes) key
    SecretKey secretKey = AbstractCipher.newAESKey();

    byte[] encryptedText = cipher.doEncrypt(pText.getBytes(StandardCharsets.UTF_8), secretKey);
    String decryptedText = cipher.doDecrypt(encryptedText, secretKey);

    assertEquals(pText, decryptedText);
  }

  @Test
  public void testEncryptDecryptWithDekKek() throws Exception {

    String plainText = "Use AES-GCM, and cipher and decipher the plainText!";

    DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(
            DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    AbstractCipher cipher = getCipher(config);
    // create DEK and IV to encrypt plainText
    SecretKey dek = AbstractCipher.newAESKey();

    // encrypt the pText
    byte[] encryptedText = cipher.doEncrypt(plainText.getBytes(StandardCharsets.UTF_8), dek);

    // create KEK and IV to encrypt DEK
    SecretKey kek = LocalCipher.newAESKey();

    // encrypt DEK
    String sdek = Base64.getEncoder().encodeToString(dek.getEncoded());
    byte[] encryptedDEK = cipher.doEncrypt(sdek.getBytes(StandardCharsets.UTF_8), kek);

    // decrypt DEK through KEK
    String sdecryptedDek = cipher.doDecrypt(encryptedDEK, kek);
    // decode the base64 encoded string
    byte[] decodedKey = Base64.getDecoder().decode(sdecryptedDek);
    // rebuild DEK using SecretKeySpec
    SecretKey originalKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");

    // decrypt plainText
    String decryptedText = cipher.doDecrypt(encryptedText, originalKey);

    assertEquals(plainText, decryptedText);
  }

  @Test(expected = CredentialsException.class)
  public void testInvalidToken() throws Exception {

    String pText = "Use AES-GCM for encryption and decryption!";

    DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(
            DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    AbstractCipher cipher = getCipher(config);
    // get AES 256 bits (32 bytes) key
    SecretKey secretKey = AbstractCipher.newAESKey();

    byte[] encryptedText = cipher.doEncrypt(pText.getBytes(StandardCharsets.UTF_8), secretKey);
    String decryptedText =
        cipher.doDecrypt(Arrays.copyOfRange(encryptedText, 0, encryptedText.length - 2), secretKey);
  }

  @Test(expected = CredentialsException.class)
  public void testUnderflowToken() throws Exception {

    String pText = "Use AES-GCM for encryption and decryption!";

    DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(
            DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    AbstractCipher cipher = getCipher(config);
    // get AES 256 bits (32 bytes) key
    SecretKey secretKey = AbstractCipher.newAESKey();

    byte[] encryptedText = cipher.doEncrypt(pText.getBytes(StandardCharsets.UTF_8), secretKey);
    String decryptedText = cipher.doDecrypt(Arrays.copyOfRange(encryptedText, 0, 20), secretKey);
  }

  @Test
  public void testInvalidAlias() throws Exception {

    String pText = "Use AES-GCM for encryption and decryption!";

    DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(
            DremioConfig.LOCAL_WRITE_PATH_STRING, tempFolder.newFolder().toString());
    AbstractCipher cipher = getCipher(config);
    SecretKey key = cipher.lookupKeystore("master", true);
    assertNotNull(key);
    key = cipher.lookupKeystore("master", false);
    assertNotNull(key);

    assertThatThrownBy(() -> cipher.lookupKeystore("invalidAlias", false))
        .isInstanceOf(NoSuchElementException.class);
  }
}

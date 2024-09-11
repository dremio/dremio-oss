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

import static com.dremio.config.DremioConfig.CREDENTIALS_KEYSTORE_PASSWORD;

import com.dremio.config.DremioConfig;
import com.dremio.security.SecurityFolder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * An Abstract secrets cipher that implements base logic and configuration for encrypting and
 * decrypting secrets using a DEK/KEK pattern. Fields and methods can be pushed down to
 * implementations as needed.
 */
public abstract class AbstractCipher implements Cipher {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AbstractCipher.class);
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final String AES = "AES";
  private static final int INITIALIZATION_VECTOR_LENGTH_IN_BYTES = 12;
  private static final int AES_KEY_SIZE = 256;

  private static final int KEYSTORE_KEK_ENTRY = 1;
  private static final String KEYSTORE_KEK_ENTRY_ALIAS = String.valueOf(KEYSTORE_KEK_ENTRY);
  private static final String KEYSTORE_TYPE = "PKCS12";
  private static final String ENCRYPT_ALGO = "AES/GCM/NoPadding";
  private static final int TAG_LENGTH_IN_BITS = 128;

  private static final String OPAQUE_ID = String.valueOf(KEYSTORE_KEK_ENTRY);
  private static final String SECURE_URI_SPLITTER = ".";
  private static final String SECURE_LOOKUP_URI_SPLITTER = "\\.";
  private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
  private static final Base64.Encoder ENCODER = Base64.getUrlEncoder();

  // This is the default Dremio keystore password. An external password should be used to provide
  // more security.
  private static final String DEFAULT_KEYSTORE_PASSWORD = "unsecurepassword";

  private static final String ILLEGAL_ARGUMENT_ERROR_MESSAGE = "Unknown encrypted secret format";

  protected abstract DremioConfig getConfig();

  protected abstract CredentialsService getCredentialsService();

  protected abstract String getKeystoreFilename();

  protected abstract String getScheme();

  /** Encryption with prefix IV length + IV bytes to cipher text */
  protected byte[] doEncrypt(byte[] plainText, SecretKey key) throws CredentialsException {

    if (plainText.length > (Integer.MAX_VALUE - INITIALIZATION_VECTOR_LENGTH_IN_BYTES)) {
      String errorMsg = String.format("The length of secret %d is too big ", plainText.length);
      throw new IllegalArgumentException(errorMsg);
    }

    final byte[] initVector = newInitVector();
    byte[] cipherText;

    try {
      javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance(ENCRYPT_ALGO);
      cipher.init(
          javax.crypto.Cipher.ENCRYPT_MODE,
          key,
          new GCMParameterSpec(TAG_LENGTH_IN_BITS, initVector));
      cipherText = cipher.doFinal(plainText);
    } catch (GeneralSecurityException e) {
      throw new SecretCredentialsException("Secret encryption encounters exception", e);
    }

    return ByteBuffer.allocate(initVector.length + cipherText.length)
        .put(initVector)
        .put(cipherText)
        .array();
  }

  /** Decryption with prefix IV length + IV bytes to decipher text */
  protected String doDecrypt(byte[] cipherTextWithIV, SecretKey key) throws CredentialsException {

    try {
      ByteBuffer bb = ByteBuffer.wrap(cipherTextWithIV);

      byte[] initVector = new byte[INITIALIZATION_VECTOR_LENGTH_IN_BYTES];
      bb.get(initVector);

      byte[] cipherText = new byte[bb.remaining()];
      bb.get(cipherText);

      javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance(ENCRYPT_ALGO);
      cipher.init(
          javax.crypto.Cipher.DECRYPT_MODE,
          key,
          new GCMParameterSpec(TAG_LENGTH_IN_BITS, initVector));
      byte[] plainText = cipher.doFinal(cipherText);
      return new String(plainText, StandardCharsets.UTF_8);
    } catch (GeneralSecurityException | BufferUnderflowException e) {
      throw new SecretCredentialsException("Secret decryption encounters exception", e);
    }
  }

  /** new Initialization Vector */
  private static byte[] newInitVector() {
    byte[] nonce = new byte[INITIALIZATION_VECTOR_LENGTH_IN_BYTES];
    new SecureRandom().nextBytes(nonce);
    return nonce;
  }

  /** new AES secret key */
  protected static SecretKey newAESKey() throws NoSuchAlgorithmException {
    KeyGenerator keyGen = KeyGenerator.getInstance(AES);
    keyGen.init(AES_KEY_SIZE, SECURE_RANDOM);
    return keyGen.generateKey();
  }

  /** Build secret token */
  @Override
  public String encrypt(String secret) throws CredentialsException {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      SecretKey kek = lookupKeystore(KEYSTORE_KEK_ENTRY_ALIAS, true);
      SecretKey dek = newAESKey();

      String encodedDEK = ENCODER.encodeToString(dek.getEncoded());

      byte[] encryptedSecretBytes = doEncrypt(secret.getBytes(StandardCharsets.UTF_8), dek);
      byte[] encryptedDekBytes = doEncrypt(encodedDEK.getBytes(StandardCharsets.UTF_8), kek);

      String encryptedSecretString = ENCODER.encodeToString(encryptedSecretBytes);
      String encryptedDekString = ENCODER.encodeToString(encryptedDekBytes);

      logger.debug("Encryption took {} milliseconds.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      return OPAQUE_ID
          + SECURE_URI_SPLITTER
          + encryptedDekString
          + SECURE_URI_SPLITTER
          + encryptedSecretString;
    } catch (GeneralSecurityException e) {
      logger.debug(
          "Encryption failed and took {} milliseconds.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      throw new SecretCredentialsException("Building secret token encounters exception.", e);
    }
  }

  @Override
  public String decrypt(String token) throws CredentialsException {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      SecretKey kek = lookupKeystore(KEYSTORE_KEK_ENTRY_ALIAS, false);
      // split the encrypted secret to encrypted opaque_id, encrypted DEK, and encrypted secret
      String[] args = token.split(SECURE_LOOKUP_URI_SPLITTER, 3);

      if (args.length != 3) {
        throw new IllegalArgumentException(ILLEGAL_ARGUMENT_ERROR_MESSAGE);
      }

      String opaqueId = args[0];
      String encryptedDekString = args[1];
      String encryptedSecretString = args[2];

      if (!OPAQUE_ID.equals(opaqueId)) {
        throw new IllegalArgumentException(ILLEGAL_ARGUMENT_ERROR_MESSAGE);
      }

      byte[] encryptedDekBytesWithIV = DECODER.decode(encryptedDekString);

      String encodedDEK = doDecrypt(encryptedDekBytesWithIV, kek);
      byte[] decodedDEKBytes = DECODER.decode(encodedDEK);
      // rebuild DEK using SecretKeySpec
      SecretKey dek = new SecretKeySpec(decodedDEKBytes, 0, decodedDEKBytes.length, AES);

      byte[] secretBytesWithIV = DECODER.decode(encryptedSecretString);
      String secret = doDecrypt(secretBytesWithIV, dek);
      logger.debug("Decryption took {} milliseconds.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      return secret;
    } catch (NoSuchElementException e) {
      logger.debug(
          "Decryption failed and took {} milliseconds.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      throw new SecretCredentialsException("Cannot decode token");
    }
  }

  /** Lookup keystore password from password URI */
  private char[] getKeystorePassword() throws CredentialsException {
    final String keystorePasswordUri = getConfig().getString(CREDENTIALS_KEYSTORE_PASSWORD);
    if (Strings.isNullOrEmpty(keystorePasswordUri)) {
      return DEFAULT_KEYSTORE_PASSWORD.toCharArray();
    }

    URI uri;
    try {
      uri = CredentialsServiceUtils.safeURICreate(keystorePasswordUri);
    } catch (IllegalArgumentException e) {
      logger.debug("The string used to locate secret is not a valid URI.");
      return keystorePasswordUri.toCharArray();
    }

    final String scheme = uri.getScheme();
    if (scheme == null) {
      return keystorePasswordUri.toCharArray();
    }

    if (getScheme().equalsIgnoreCase(scheme)) {
      throw new SecretCredentialsException("Cannot use secret URI for Dremio keystore password.");
    }
    return getCredentialsService().lookup(keystorePasswordUri).toCharArray();
  }

  /** Look up secret key in Dremio Keystore */
  @VisibleForTesting
  SecretKey lookupKeystore(String alias, boolean create) throws CredentialsException {
    boolean keystoreExists = SecurityFolder.exists(getConfig(), getKeystoreFilename());
    if (!keystoreExists && !create) {
      throw new SecretCredentialsException("Keystore not found.");
    }

    try {
      SecurityFolder securityFolder = SecurityFolder.of(getConfig());
      final char[] password = getKeystorePassword();
      if (keystoreExists) {
        final KeyStore keystore = KeyStore.getInstance(KEYSTORE_TYPE);
        try (final InputStream inputStream =
            securityFolder.newSecureInputStream(getKeystoreFilename())) {
          keystore.load(inputStream, password);
        }
        KeyStore.ProtectionParameter entryPassword = new KeyStore.PasswordProtection(password);
        KeyStore.SecretKeyEntry entry =
            (KeyStore.SecretKeyEntry) keystore.getEntry(alias, entryPassword);
        if (entry == null) {
          throw new NoSuchElementException("No key found");
        }
        return entry.getSecretKey();
      }

      // Bootstrap a key
      final KeyStore keystore = KeyStore.getInstance(KEYSTORE_TYPE);
      keystore.load(null, null);

      SecretKey kek = newAESKey();
      KeyStore.SecretKeyEntry secretKeyEntry = new KeyStore.SecretKeyEntry(kek);
      KeyStore.ProtectionParameter entryPassword = new KeyStore.PasswordProtection(password);
      keystore.setEntry(alias, secretKeyEntry, entryPassword);

      try (OutputStream outputStream =
          securityFolder.newSecureOutputStream(
              getKeystoreFilename(), SecurityFolder.OpenOption.CREATE_ONLY)) {
        keystore.store(outputStream, password);
      }
      return kek;
    } catch (GeneralSecurityException | IOException e) {
      throw new SecretCredentialsException("Encounter exception in looking up keystore.", e);
    }
  }
}

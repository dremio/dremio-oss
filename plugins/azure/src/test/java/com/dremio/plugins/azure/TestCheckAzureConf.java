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
package com.dremio.plugins.azure;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SecretRef;
import com.google.common.collect.ImmutableSet;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for the {@code CheckAzureConf} constraint, which is validated by the {@code
 * CheckAzureConfValidator}.
 */
public class TestCheckAzureConf {
  private static Validator validator;

  private static final SecretRef VALID_ACCESS_KEY =
      () -> Base64.encode("access_key".getBytes(Charset.defaultCharset()));
  private static final SecretRef INVALID_ACCESS_KEY =
      () -> "access%Key"; // This is not a URL or a valid azure access key.
  private static final String AZURE_STORAGE_ACCOUNT_NAME = "azurestorage";

  @BeforeClass
  public static void setUp() {
    final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();
  }

  @Test
  public void testConfigWithInvalidKey() {
    final AzureStorageConf conf = new AzureStorageConf();
    conf.accountName = AZURE_STORAGE_ACCOUNT_NAME;
    conf.accessKey = INVALID_ACCESS_KEY;
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertEquals(1, violations.size());
    final ConstraintViolation<AzureStorageConf> data = violations.iterator().next();
    Assert.assertEquals("Invalid credentials provided.", data.getMessage());
    Assert.assertEquals("accessKey", data.getPropertyPath().iterator().next().toString());
  }

  @Test
  public void testValidAzureConfig() {
    final AzureStorageConf conf = new AzureStorageConf();
    conf.accessKey = VALID_ACCESS_KEY;
    conf.accountName = AZURE_STORAGE_ACCOUNT_NAME;
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertTrue(violations.isEmpty());
  }

  @Test
  public void testValidAzureAccessKeyURL() {
    final AzureStorageConf conf = new AzureStorageConf();
    // This is a URL to an azure access key but the string itself is not a valid azure access key.
    conf.accessKey = () -> "azure-vault+https://test.vault.azure.net/secret";
    conf.accountName = AZURE_STORAGE_ACCOUNT_NAME;
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertTrue(violations.isEmpty());
  }

  @Test
  public void testValidAzureAccessKeyURL2() {
    final AzureStorageConf conf = new AzureStorageConf();
    // As long as the access key is an url, it will pass CheckAzureConfValidator.
    // ManagedStoragePlugin will fail when resolving the secret.
    conf.accessKey = () -> "data:text/plain;base64,invalidAccessKey";
    conf.accountName = AZURE_STORAGE_ACCOUNT_NAME;
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertTrue(violations.isEmpty());
  }

  @Test
  public void testViolationForNullAccountName() {
    final AzureStorageConf conf = new AzureStorageConf();
    conf.accessKey = VALID_ACCESS_KEY;
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertEquals(1, violations.size());
    final ConstraintViolation<AzureStorageConf> data = violations.iterator().next();
    Assert.assertEquals("Azure storage account name is missing.", data.getMessage());
    Assert.assertEquals("accountName", data.getPropertyPath().iterator().next().toString());
  }

  @Test
  public void testViolationForEmptyAccountName() {
    final AzureStorageConf conf = new AzureStorageConf();
    conf.accessKey = VALID_ACCESS_KEY;
    conf.accountName = "";
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertEquals(1, violations.size());
    final ConstraintViolation<AzureStorageConf> data = violations.iterator().next();
    Assert.assertEquals("Azure storage account name is missing.", data.getMessage());
    Assert.assertEquals("accountName", data.getPropertyPath().iterator().next().toString());
  }

  @Test
  public void testViolationForNullAccessKey() {
    final AzureStorageConf conf = new AzureStorageConf();
    conf.accountName = AZURE_STORAGE_ACCOUNT_NAME;
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertEquals(1, violations.size());
    final ConstraintViolation<AzureStorageConf> data = violations.iterator().next();
    Assert.assertEquals("Azure storage account access key is missing.", data.getMessage());
    Assert.assertEquals("accessKey", data.getPropertyPath().iterator().next().toString());
  }

  @Test
  public void testViolationForEmptyAccessKey() {
    final AzureStorageConf conf = new AzureStorageConf();
    conf.accountName = AZURE_STORAGE_ACCOUNT_NAME;
    conf.accessKey = SecretRef.empty();
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertEquals(1, violations.size());
    final ConstraintViolation<AzureStorageConf> data = violations.iterator().next();
    Assert.assertEquals("Azure storage account access key is missing.", data.getMessage());
    Assert.assertEquals("accessKey", data.getPropertyPath().iterator().next().toString());
  }

  @Test
  public void testAzureConfigWithAADCredentialType() {
    final AzureStorageConf conf = new AzureStorageConf();
    conf.accountName = AZURE_STORAGE_ACCOUNT_NAME;
    conf.accessKey = INVALID_ACCESS_KEY;
    conf.credentialsType = AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY;
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertTrue(violations.isEmpty());
  }

  @Test
  public void testAccessKeyWhichThrowsIllegalArgumentException() {
    final AzureStorageConf conf = new AzureStorageConf();
    conf.accountName = AZURE_STORAGE_ACCOUNT_NAME;
    conf.accessKey =
        () -> "SecretString%.isNotAValidURI"; // This is not a URL or a valid azure access key.
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertEquals(1, violations.size());
    final ConstraintViolation<AzureStorageConf> data = violations.iterator().next();
    Assert.assertEquals("Invalid credentials provided.", data.getMessage());
    Assert.assertEquals("accessKey", data.getPropertyPath().iterator().next().toString());
  }

  @Test
  public void testAccessKeyUseExistingValue() {
    final AzureStorageConf conf = new AzureStorageConf();
    conf.accountName = AZURE_STORAGE_ACCOUNT_NAME;
    conf.accessKey = () -> ConnectionConf.USE_EXISTING_SECRET_VALUE;
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    Assert.assertTrue(violations.isEmpty());
  }

  @Test
  public void testCheckCredentialsMissing() {
    final AzureStorageConf conf = new AzureStorageConf();
    final Set<ConstraintViolation<AzureStorageConf>> violations = validator.validate(conf);
    final Set<String> violationMessages =
        violations.stream().map(ConstraintViolation::getMessage).collect(Collectors.toSet());
    Assert.assertEquals(2, violationMessages.size());
    Assert.assertTrue(
        violationMessages.containsAll(
            ImmutableSet.of(
                "Azure storage account name is missing.",
                "Azure storage account access key is missing.")));
  }
}

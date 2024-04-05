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

import com.dremio.exec.catalog.conf.AbstractSecretRef;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.CredentialsServiceUtils;
import com.google.common.base.Strings;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;

/** Validator for the {@code CheckAzureConf} annotation. */
public class CheckAzureConfValidator
    implements ConstraintValidator<CheckAzureConf, AbstractAzureStorageConf> {

  @Override
  public boolean isValid(AbstractAzureStorageConf value, ConstraintValidatorContext context) {
    if (value == null || value.credentialsType == AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY) {
      return true;
    }

    context.disableDefaultConstraintViolation();
    final String key;
    if (value.getSharedAccessSecretType() == SharedAccessSecretType.SHARED_ACCESS_SECRET_KEY) {
      // Do not raise a constraint violation if we are editing an existing source (which
      // would write a placeholder in the accessKey field).
      if (SecretRef.EXISTING_VALUE.equals(value.accessKey)) {
        return true;
      }
      key = unpackAccessKey(value);
    } else { // Azure Key Vault
      key = value.getAccessKeyUri();
    }
    final String account = value.accountName;

    boolean credentialsPresent = true;

    if (Strings.isNullOrEmpty(account)) {
      context
          .buildConstraintViolationWithTemplate("Azure storage account name is missing.")
          .addPropertyNode("accountName")
          .addConstraintViolation();
      credentialsPresent = false;
    }

    if (Strings.isNullOrEmpty(key)) {
      context
          .buildConstraintViolationWithTemplate("Azure storage account access key is missing.")
          .addPropertyNode("accessKey")
          .addConstraintViolation();
      credentialsPresent = false;
    }

    // If any of the credentials are missing, at this point we have constraint violations
    // for either case (or both), in which case just set the return value to false.
    // If both credentials are present, then try constructing the SharedCredentialKey
    // with the account name and access key.
    if (!credentialsPresent) {
      return false;
    }

    try {
      try {
        CredentialsServiceUtils.safeURICreate(key);
      } catch (IllegalArgumentException e) {
        // If the key is not a URL, then we go through the azure access key check.
        new SharedKeyCredentials(account, key);
      }
      return true;
    } catch (StringIndexOutOfBoundsException | IllegalArgumentException e) {
      context
          .buildConstraintViolationWithTemplate("Invalid credentials provided.")
          .addPropertyNode("accessKey")
          .addConstraintViolation();
      return false;
    }
  }

  /**
   * We are performing validation of user-input, unpack raw SecretRef value. Do not use in other
   * contexts. Performs get on cloned Conf to ensure no side effects.
   */
  private String unpackAccessKey(AbstractAzureStorageConf conf) {
    if (SecretRef.isNullOrEmpty(conf.accessKey)) {
      return null; // we handle null and empty the same way, so collapsing to null is fine
    }
    final String accessKey;
    if (conf.accessKey instanceof AbstractSecretRef) {
      final AbstractAzureStorageConf cloned =
          conf.clone(); // AbstractSecretRef is known to be cloneable
      ((AbstractSecretRef) cloned.accessKey).decorateSecrets(new NoopCredentialsService());
      accessKey = cloned.accessKey.get();
    } else {
      accessKey = conf.accessKey.get();
    }
    return accessKey;
  }

  /**
   * Credentials Service that just returns the given pattern. Only for performing user-input
   * validation. Do not use in other contexts.
   */
  private static final class NoopCredentialsService implements CredentialsService {
    @Override
    public String lookup(String pattern) {
      return pattern;
    }

    @Override
    public void start() {}

    @Override
    public void close() {}
  }
}

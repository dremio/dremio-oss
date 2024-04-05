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

import static com.dremio.plugins.azure.AzureStorageFileSystem.ACCOUNT;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.dremio.common.exceptions.UserException;
import com.dremio.services.credentials.CredentialsException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.asynchttpclient.Request;

/**
 * Represents the shared key credentials used to access an Azure Storage account. Instead of using
 * raw account key, looks up key using credentials service.
 */
public class AzureSharedKeyCredentials implements AzureStorageCredentials, Configurable {

  // TODO: Make configurable (need new configuration option in conf)
  @VisibleForTesting
  static final long SECRET_TTL = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  private static final LookupResult INVALID_SECRET = new LookupResult();
  private Supplier<String> accountKeySupplier;
  private String accountName;
  private volatile Configuration conf;
  private volatile LookupResult secret;

  /**
   * Constructor for {@link org.apache.hadoop.util.ReflectionUtils#newInstance}. Used by
   * hadoop-azure to provide a custom request signer.
   */
  public AzureSharedKeyCredentials() {
    this.secret = INVALID_SECRET; // set as placeholder
  }

  @VisibleForTesting
  AzureSharedKeyCredentials(final String accountName, Supplier<String> accountKeySupplier) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(accountName), "Invalid account name");
    this.accountName = accountName;
    this.accountKeySupplier =
        Preconditions.checkNotNull(accountKeySupplier, "Invalid account key supplier");
    this.conf = null;
    this.secret = INVALID_SECRET;
  }

  private boolean isExpired() {
    // This is used to check if a refresh is needed in the first place, so do not trigger a refresh
    return secret.getCreatedOn().getTime() + SECRET_TTL < System.currentTimeMillis();
  }

  @Override
  public boolean checkAndUpdateToken() {
    if (isExpired()) {
      secret = new LookupResult(getAccountName(), getAccountKeySupplier().get());
      return true;
    }
    return false;
  }

  @Override
  public String getAuthzHeaderValue(final Request req) {
    final StorageSharedKeyCredential storageSharedKeyCredential =
        getSecret().getStorageSharedKeyCredential();
    try {
      final URL url = new URL(req.getUrl());
      final Map<String, String> headersMap = new HashMap<>();
      req.getHeaders().forEach(header -> headersMap.put(header.getKey(), header.getValue()));
      return storageSharedKeyCredential.generateAuthorizationHeader(
          url, req.getMethod(), headersMap);
    } catch (MalformedURLException e) {
      throw new IllegalStateException("The request URL is invalid ", e);
    }
  }

  @Override
  public void signRequest(final HttpURLConnection connection, final long contentLength)
      throws UnsupportedEncodingException {
    getSecret().getSharedKeyCredentials().signRequest(connection, contentLength);
  }

  @Override
  public void setConf(final Configuration conf) {
    Preconditions.checkNotNull(conf);
    getSharedAccessKey(conf); // pre-check

    synchronized (this) {
      this.conf = conf;
      this.accountKeySupplier = () -> getSharedAccessKey(conf);
      this.accountName = Objects.requireNonNull(conf.get(ACCOUNT));
    }
  }

  /** Get Secret, updating if necessary */
  @VisibleForTesting
  protected LookupResult getSecret() {
    checkAndUpdateToken();
    return secret;
  }

  @VisibleForTesting
  protected void setSecret(LookupResult secret) {
    this.secret = secret;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private Supplier<String> getAccountKeySupplier() {
    return this.accountKeySupplier;
  }

  private String getAccountName() {
    return this.accountName;
  }

  @Override
  public StorageCredentials exportToStorageCredentials() {
    return getSecret().getStorageCredentials();
  }

  /**
   * Get the shared access key from configuration. Unpacks exceptions such that cause of
   * CredentialsException is surfaced.
   */
  static String getSharedAccessKey(Configuration conf) {
    try {
      return new String(Objects.requireNonNull(conf.getPassword(AzureStorageFileSystem.KEY)));
    } catch (IOException e) {
      // Hadoop does a lot of exception wrapping, so dig to find actual error.
      // Throw UserException to surface problems with credentials config to the user
      for (Throwable throwable : Throwables.getCausalChain(e)) {
        if (throwable instanceof CredentialsException) {
          final String message =
              throwable.getCause() == null
                  ? throwable.getMessage()
                  : throwable.getCause().getMessage();
          throw UserException.permissionError(throwable)
              .message("Failed to resolve credentials: " + message)
              .buildSilently();
        }
      }
      throw UserException.permissionError(e)
          .message("Failed to resolve credentials.")
          .buildSilently();
    }
  }

  @VisibleForTesting
  protected static class LookupResult {
    private final String accountName;
    private final String secret;
    private final Date createdOn;

    private final Supplier<SharedKeyCredentials> sharedKeyCredentials =
        Suppliers.memoize(() -> new SharedKeyCredentials(getAccountName(), getSecret()));
    private final Supplier<StorageCredentials> storageCredentials =
        Suppliers.memoize(() -> new StorageCredentialsAccountAndKey(getAccountName(), getSecret()));
    private final Supplier<StorageSharedKeyCredential> storageSharedKeyCredential =
        Suppliers.memoize(() -> new StorageSharedKeyCredential(getAccountName(), getSecret()));

    public LookupResult(String accountName, String secret) {
      this.accountName = accountName;
      this.secret = secret;
      this.createdOn = new Date();
    }

    private LookupResult() {
      this.accountName = "";
      this.secret = "";
      this.createdOn = new Date(0);
    }

    @VisibleForTesting
    LookupResult(String accountName, String secret, long createdOn) {
      this.accountName = accountName;
      this.secret = secret;
      this.createdOn = new Date(createdOn);
    }

    public String getAccountName() {
      return accountName;
    }

    public String getSecret() {
      return secret;
    }

    public Date getCreatedOn() {
      return createdOn;
    }

    public SharedKeyCredentials getSharedKeyCredentials() {
      return sharedKeyCredentials.get();
    }

    public StorageCredentials getStorageCredentials() {
      return storageCredentials.get();
    }

    public StorageSharedKeyCredential getStorageSharedKeyCredential() {
      return storageSharedKeyCredential.get();
    }
  }
}

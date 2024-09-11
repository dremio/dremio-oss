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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.asynchttpclient.Request;

/**
 * Represents the shared key credentials used to access an Azure Storage account. Instead of using
 * raw account key, looks up key using credentials service.
 */
public class AzureSharedKeyCredentials implements AzureStorageCredentials, Configurable {

  private Supplier<String> accountKeySupplier;
  private String accountName;
  private volatile Configuration conf;

  /**
   * Constructor for {@link org.apache.hadoop.util.ReflectionUtils#newInstance}. Used by
   * hadoop-azure to provide a custom request signer.
   */
  public AzureSharedKeyCredentials() {}

  @VisibleForTesting
  AzureSharedKeyCredentials(final String accountName, Supplier<String> accountKeySupplier) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(accountName), "Invalid account name");
    Preconditions.checkNotNull(accountKeySupplier, "Invalid account key supplier");
    this.accountName = accountName;
    this.accountKeySupplier = memorizeAccountKeySupplier(accountKeySupplier);
    this.conf = null;
  }

  private Supplier<String> memorizeAccountKeySupplier(Supplier<String> accountKeySupplier) {
    return Suppliers.memoizeWithExpiration(accountKeySupplier, 5, TimeUnit.MINUTES);
  }

  @Override
  public boolean checkAndUpdateToken() {
    return false;
  }

  @Override
  public String getAuthzHeaderValue(final Request req) {
    StorageSharedKeyCredential storageSharedKeyCredential =
        new StorageSharedKeyCredential(accountName, accountKeySupplier.get());
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
    new SharedKeyCredentials(accountName, accountKeySupplier.get())
        .signRequest(connection, contentLength);
  }

  @Override
  public void setConf(final Configuration conf) {
    Preconditions.checkNotNull(conf);
    getSharedAccessKey(conf); // pre-check

    synchronized (this) {
      this.conf = conf;
      this.accountKeySupplier = memorizeAccountKeySupplier(() -> getSharedAccessKey(conf));
      this.accountName = Objects.requireNonNull(conf.get(ACCOUNT));
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public StorageCredentialsAccountAndKey exportToStorageCredentials() {
    return new StorageCredentialsAccountAndKey(accountName, accountKeySupplier.get());
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
}

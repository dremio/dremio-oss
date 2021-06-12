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

import java.io.IOException;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.plugins.util.ContainerFileSystem;
import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.blob.CloudBlobClient;

/**
 * A BlobContainerProvider that uses OAuth with Azure Active Directory
 */
public class BlobContainerProviderOAuth extends BlobContainerProvider {
  private static final Logger logger = LoggerFactory.getLogger(BlobContainerProviderOAuth.class);

  private AzureOAuthTokenProvider tokenProvider;

  public BlobContainerProviderOAuth(AzureStorageFileSystem parent, String connection, String account,
                                    AzureOAuthTokenProvider tokenGenerator, String[] containers) throws Exception {
    super(parent, account, connection, new StorageCredentialsToken(account, tokenGenerator.getToken()), true, containers);
    this.tokenProvider = tokenGenerator;
  }


  public BlobContainerProviderOAuth(AzureStorageFileSystem parent, String connection, String account,
                                    AzureOAuthTokenProvider tokenGenerator) throws Exception {
    super(parent, account, connection, new StorageCredentialsToken(account, tokenGenerator.getToken()), true, null);
    this.tokenProvider = tokenGenerator;
  }

  @Override
  public Stream<ContainerFileSystem.ContainerCreator> getContainerCreators() throws IOException {
    try {
      synchronized (this){
        if(tokenProvider.checkAndUpdateToken()) {
          logger.debug("Storage V1 - BlobContainerProviderOAuth - Token is expired or is about to expire, token has been updated");
          CloudBlobClient newClient = new CloudBlobClient(getConnection(),
            new StorageCredentialsToken(getAccount(), tokenProvider.getToken()));
          setCloudBlobClient(newClient);
        }
      }
    } catch(Exception ex) {
      throw new IOException("Unable to update client token: ", ex);
    }
    return super.getContainerCreators();
  }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.v10.adlsg2.models.FilesystemListResponse;
import com.microsoft.azure.storage.v10.adlsg2.models.PathReadResponse;
import com.microsoft.azure.storage.v10.blob.TokenCredentials;

import io.reactivex.Single;

/**
 * Client extending DataLakeG2Client that supports Azure AD OAuth connections
 */
public class DataLakeG2OAuthClient extends DataLakeG2Client {
  private static final Logger logger = LoggerFactory.getLogger(DataLakeG2OAuthClient.class);

  private final AzureTokenGenerator tokenGenerator;

  public DataLakeG2OAuthClient(String account, AzureTokenGenerator tokenGenerator, boolean secure, String azureEndpoint) {
    super(new TokenCredentials(tokenGenerator.getToken()), account, secure, azureEndpoint);
    this.tokenGenerator = tokenGenerator;
  }

  @Override
  public Single<FilesystemListResponse> listFilesystems(
    String prefix,
    String continuation) throws Exception {
    checkAndUpdateClient();
    return super.listFilesystems(prefix, continuation);
  }

  @Override
  public Single<PathReadResponse> read(
    String filesystem,
    String path,
    long startInclusive,
    long endExclusive) throws Exception {
    checkAndUpdateClient();
    return super.read(filesystem, path, startInclusive, endExclusive);
  }

  private synchronized void checkAndUpdateClient() throws Exception {
    if (tokenGenerator.checkAndUpdateToken()) {
      logger.debug("Storage V2 - DataLakeG2OAuthClient - Token is expired or is about to expire, token has been updated");
      generateAndSetDataLakeStorageClient(new TokenCredentials(tokenGenerator.getToken()));
    }
  }
}

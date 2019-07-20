/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.security.InvalidKeyException;

import com.microsoft.azure.storage.v10.adlsg2.GeneratedDataLakeStorageClient;
import com.microsoft.azure.storage.v10.adlsg2.models.FilesystemListResponse;
import com.microsoft.azure.storage.v10.adlsg2.models.PathReadResponse;
import com.microsoft.azure.storage.v10.blob.ICredentials;
import com.microsoft.azure.storage.v10.blob.PipelineOptions;
import com.microsoft.azure.storage.v10.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.v10.blob.StorageURL;
import com.microsoft.azure.storage.v10.blob.Utility;
import com.microsoft.rest.v2.Context;

import io.reactivex.Single;

/**
 * Client to wrap generated swagger/autorest code.
 */
class DataLakeG2Client {

  private final GeneratedDataLakeStorageClient client;
  public DataLakeG2Client(ICredentials credentials, String account, boolean secure, String azureEndpoint) {
    this.client = new GeneratedDataLakeStorageClient(StorageURL.createPipeline(credentials, new PipelineOptions()))
        .withAccountName(account)
        .withScheme(secure ? "https" : "http")
        .withDnsSuffix(azureEndpoint)
        .withXMsVersion("2018-11-09");
  }

  public DataLakeG2Client(String account, String key, boolean secure, String azureEndpoint) throws InvalidKeyException {
    this(new SharedKeyCredentials(account, key), account, secure, azureEndpoint);
  }

  public Single<FilesystemListResponse> listFilesystems(
      String prefix,
      String continuation) {
    return Utility.addErrorWrappingToSingle(
        client.generatedFilesystems().listWithRestResponseAsync(Context.NONE, prefix, continuation, 5000, null,
            null, null));

  }

  public Single<PathReadResponse> read(
      String filesystem,
      String path,
      long startInclusive,
      long endExclusive) {
    String range = String.format("bytes=%d-%d", startInclusive, endExclusive - 1);
    return Utility.addErrorWrappingToSingle(
        client.generatedPaths().readWithRestResponseAsync(Context.NONE, filesystem, path, range, null, null, null, null, null, null, null, null));
  }

}

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
package com.dremio.plugins.gcs;

import com.dremio.http.AsyncHttpClientProvider;
import com.dremio.io.AsyncByteReader;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.function.Supplier;
import org.apache.commons.lang3.function.Suppliers;
import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncHttpClient;

/** Helper class for constructor and maintaining AsyncHttpClients. */
class GCSAsyncClient implements Closeable {
  private static final String SCOPE = "https://www.googleapis.com/auth/devstorage.full_control";

  private final AsyncHttpClient asyncHttpClient;
  private final Supplier<GoogleCredentials> credentialsProvider;
  private final String baseUrl;
  private final GcsApiType gcsApiType;

  public GCSAsyncClient(
      String name,
      Supplier<GoogleCredentials> credentialsProvider,
      String baseUrl,
      GcsApiType gcsApiType) {
    this.credentialsProvider = credentialsProvider;
    this.asyncHttpClient = AsyncHttpClientProvider.getInstance();
    this.baseUrl = baseUrl;
    this.gcsApiType = gcsApiType;
  }

  private GoogleCredentials createScopedCredentials() {
    GoogleCredentials credentials = Suppliers.get(credentialsProvider);
    return credentials == null ? null : credentials.createScoped(Collections.singletonList(SCOPE));
  }

  public AsyncByteReader newByteReader(Path path, String version) {
    return new GCSAsyncFileReader(
        asyncHttpClient, path, version, createScopedCredentials(), baseUrl, gcsApiType);
  }

  @Override
  public void close() throws IOException {}
}

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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncHttpClient;

import com.dremio.http.AsyncHttpClientProvider;
import com.dremio.io.AsyncByteReader;
import com.google.auth.oauth2.GoogleCredentials;

/**
 * Helper class for constructor and maintaining AsyncHttpClients.
 */
class GCSAsyncClient implements Closeable {
  private static final String SCOPE = "https://www.googleapis.com/auth/devstorage.full_control";


  private final AsyncHttpClient asyncHttpClient;
  private final GoogleCredentials credentials;

  public GCSAsyncClient(String name, GoogleCredentials credentials) throws IOException {
    this.credentials = credentials.createScoped(Collections.singletonList(SCOPE));
    asyncHttpClient = AsyncHttpClientProvider.getInstance();
  }

  public AsyncByteReader newByteReader(Path path, String version) {
    return new GCSAsyncFileReader(asyncHttpClient, path, version, credentials);
  }

  @Override
  public void close() throws IOException {
  }

}

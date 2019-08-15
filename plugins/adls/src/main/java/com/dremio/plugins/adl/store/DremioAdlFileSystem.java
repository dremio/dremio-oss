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
package com.dremio.plugins.adl.store;

import java.io.IOException;

import org.apache.hadoop.fs.adl.AdlFileSystem;

import com.dremio.exec.store.dfs.async.AsyncByteReader;
import com.microsoft.azure.datalake.store.AdlsAsyncFileReader;

/**
 * Specialized Hadoop FileSystem implementation for ADLS gen 1 which adds async reading capabilities.
 */
@SuppressWarnings("Unchecked")
public class DremioAdlFileSystem extends AdlFileSystem implements AsyncByteReader.MayProvideAsyncStream {

  static final String SCHEME = "dremioAdl";
  private volatile AsyncHttpClientManager asyncHttpClientManager;

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public void close() throws IOException {
    if (asyncHttpClientManager != null) {
      asyncHttpClientManager.close();
    }
  }

  @Override
  public boolean supportsAsync() {
    return true;
  }

  @Override
  public AsyncByteReader getAsyncByteReader(AsyncByteReader.FileKey fileKey) throws IOException {
    if (asyncHttpClientManager == null) {
      synchronized (this) {
        if (asyncHttpClientManager == null) {
          final AzureDataLakeConf adlsConf = AzureDataLakeConf.fromConfiguration(getUri(), getConf());
          asyncHttpClientManager = new AsyncHttpClientManager("dist-uri-" + getUri().toASCIIString(), adlsConf);
        }
      }
    }

    return new AdlsAsyncFileReader(asyncHttpClientManager.getClient(), asyncHttpClientManager.getAsyncHttpClient(),
      fileKey.getPath().toUri().getPath());
  }
}

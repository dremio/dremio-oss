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
package com.dremio.dac.service.source;

import com.dremio.exec.catalog.VersionedListResponsePage;
import com.dremio.plugins.ExternalNamespaceEntry;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/** Holds response from a list call to a source. */
public final class ExternalListResponse {
  private Stream<ExternalNamespaceEntry> stream;
  private String pageToken;

  static ExternalListResponse ofStream(Stream<ExternalNamespaceEntry> value) {
    ExternalListResponse response = new ExternalListResponse();
    response.stream = value;
    return response;
  }

  static ExternalListResponse ofVersionedListPage(VersionedListResponsePage value) {
    ExternalListResponse response = new ExternalListResponse();
    response.stream = value.entries().stream();
    response.pageToken = value.pageToken();
    return response;
  }

  @Nullable
  public String getPageToken() {
    return pageToken;
  }

  public Stream<ExternalNamespaceEntry> getEntriesStream() {
    return stream;
  }
}

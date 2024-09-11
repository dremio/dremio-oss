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
package com.dremio.plugins;

import static com.dremio.exec.store.iceberg.model.IcebergCommitOrigin.CREATE_TABLE;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.combined.CombinedNessieClientFactory;

@ExtendWith(CombinedNessieClientFactory.class)
@NessieApiVersions(versions = NessieApiVersion.V2)
public class TestNessieClientImplWithCombinedNessieServer
    extends AbstractNessieClientImplTestWithServer {

  @BeforeEach
  void setup(NessieClientFactory clientFactory) {
    init((NessieApiV2) clientFactory.make());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testListEntriesPage(boolean withContent) {
    String parentNamespace = "listEntries" + withContent;
    int tableNum = 20;
    int pageSize = 7;
    List<ContentKey> expectedContentKeys = new ArrayList<>();
    for (int i = 0; i < tableNum; i++) {
      ContentKey key = ContentKey.of(parentNamespace, String.format("table%02d", i));
      client.commitTable(
          key.getElements(),
          "location",
          CLIENT_METADATA,
          client.getDefaultBranch(),
          null,
          CREATE_TABLE,
          "job",
          "user");
      expectedContentKeys.add(key);
    }

    List<ContentKey> actualContentKeys = new ArrayList<>();
    List<ExternalNamespaceEntry> actualEntries = new ArrayList<>();
    String pageToken = null;
    do {
      NessieListResponsePage page =
          client.listEntriesPage(
              ImmutableList.of(parentNamespace),
              client.getDefaultBranch(),
              NessieClient.NestingMode.IMMEDIATE_CHILDREN_ONLY,
              withContent
                  ? NessieClient.ContentMode.ENTRY_WITH_CONTENT
                  : NessieClient.ContentMode.ENTRY_METADATA_ONLY,
              null,
              null,
              new ImmutableNessieListOptions.Builder()
                  .setMaxResultsPerPage(pageSize)
                  .setPageToken(pageToken)
                  .build());

      assertThat(page.entries().size()).isLessThanOrEqualTo(pageSize);
      assertThat(
              page.entries().stream()
                  .allMatch(
                      e ->
                          !withContent
                                  && (e.getNessieContent() == null
                                      || e.getNessieContent().isEmpty())
                              || withContent
                                  && e.getNessieContent() != null
                                  && e.getNessieContent().isPresent()))
          .isTrue();

      for (ExternalNamespaceEntry entry : page.entries()) {
        actualContentKeys.add(ContentKey.of(entry.getNameElements()));
      }
      pageToken = page.pageToken();
    } while (pageToken != null);

    assertThat(actualContentKeys).isEqualTo(expectedContentKeys);
  }
}

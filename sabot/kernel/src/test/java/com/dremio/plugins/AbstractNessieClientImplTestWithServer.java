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
import static com.dremio.plugins.NessieUtils.createNamespaceInDefaultBranchIfNotExists;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.options.OptionManager;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;

public abstract class AbstractNessieClientImplTestWithServer {

  protected static final NessieTableAdapter CLIENT_METADATA = new NessieTableAdapter(1, 2, 3, 4);
  private static final OptionManager optionManager = Mockito.mock(OptionManager.class);

  private NessieApiV2 api;
  protected NessieClientImpl client;

  protected void init(NessieApiV2 api) {
    this.api = api;
    this.client = new NessieClientImpl(api, optionManager);
  }

  @Test
  public void testCreateNamespaces()
      throws NessieReferenceNotFoundException, NessieNamespaceNotFoundException {
    createNamespaceInDefaultBranchIfNotExists(() -> api, "test.ns.123");
    assertThat(api.getNamespace().namespace(Namespace.of("test.ns.123")).get())
        .extracting(Namespace::toContentKey)
        .isEqualTo(ContentKey.of("test.ns.123"));
  }

  @Test
  public void testGetDefaultBranch() {
    assertThat(client.getDefaultBranch().getCommitHash()).isNotBlank();
    assertThat(client.getDefaultBranch())
        .extracting(ResolvedVersionContext::isBranch, ResolvedVersionContext::getRefName)
        .containsExactly(true, "main");
  }

  @Test
  public void testCommitTable() {
    ContentKey key = ContentKey.of("test", "key123");

    ResolvedVersionContext branchBeforeCommit = client.getDefaultBranch();

    client.commitTable(
        key.getElements(),
        "loc111",
        CLIENT_METADATA,
        branchBeforeCommit,
        null,
        CREATE_TABLE,
        "job2",
        "user333");

    Optional<NessieContent> content =
        client.getContent(key.getElements(), client.getDefaultBranch(), null);
    assertThat(content)
        .isPresent()
        .get()
        .extracting(NessieContent::getCatalogKey, NessieContent::getMetadataLocation)
        .containsExactly(key.getElements(), Optional.of("loc111"));

    assertThatThrownBy(
            () ->
                client.commitTable(
                    key.getElements(),
                    "loc111",
                    CLIENT_METADATA,
                    // if we used the latest branch here we get different exceptions
                    // nessie server: NessieBadRequestException
                    // embedded nessie: still NessieReferenceConflictException
                    branchBeforeCommit,
                    null, // missing content ID for existing table
                    CREATE_TABLE,
                    "job4",
                    "user333"))
        .hasCauseInstanceOf(NessieReferenceConflictException.class)
        .hasMessageContaining("Key '" + key + "'");

    client.commitTable(
        key.getElements(),
        "loc222",
        CLIENT_METADATA,
        client.getDefaultBranch(),
        content.get().getContentId(),
        CREATE_TABLE,
        "job5",
        "user333");

    assertThat(client.getContent(key.getElements(), client.getDefaultBranch(), null))
        .isPresent()
        .get()
        .extracting(NessieContent::getCatalogKey, NessieContent::getMetadataLocation)
        .containsExactly(key.getElements(), Optional.of("loc222"));
  }
}

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
package com.dremio.plugins.dataplane.exec.gc;

import java.util.Set;
import java.util.UUID;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.iceberg.IcebergContentToContentReference;
import org.projectnessie.gc.identify.ContentTypeFilter;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.identify.IdentifyLiveContents;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.model.Content;

public class NessieLiveContentRetriever {
  private final PersistenceSpi persistenceSpi;
  private final RepositoryConnector repositoryConnector;

  NessieLiveContentRetriever(
      PersistenceSpi persistenceSpi, RepositoryConnector repositoryConnector) {
    this.persistenceSpi = persistenceSpi;
    this.repositoryConnector = repositoryConnector;
  }

  /**
   * Creates a new instance of NessieLiveContentRetriever using the given repository connector.
   *
   * @param repositoryConnector The repository connector to be used by the
   *     NessieLiveContentRetriever.
   * @return A new instance of NessieLiveContentRetriever.
   */
  public static NessieLiveContentRetriever create(RepositoryConnector repositoryConnector) {
    return new NessieLiveContentRetriever(new InMemoryPersistenceSpi(), repositoryConnector);
  }

  /**
   * Retrieves a live content set using the given cutoff policy.
   *
   * @param cutoffPolicy The cutoff policy to use for identifying live contents.
   * @return The retrieved live content set.
   * @throws LiveContentSetNotFoundException If the live content set is not found.
   */
  public LiveContentSet getLiveContentSet(CutoffPolicy cutoffPolicy)
      throws LiveContentSetNotFoundException {
    UUID uuid =
        IdentifyLiveContents.builder()
            .repositoryConnector(repositoryConnector)
            .liveContentSetsRepository(getRepository())
            .cutOffPolicySupplier(reference -> cutoffPolicy)
            .contentTypeFilter(getContentTypeFilterForIcebergTableOnly())
            .contentToContentReference(IcebergContentToContentReference.INSTANCE)
            .build()
            .identifyLiveContents();

    return getRepository().getLiveContentSet(uuid);
  }

  private LiveContentSetsRepository getRepository() {
    return LiveContentSetsRepository.builder().persistenceSpi(persistenceSpi).build();
  }

  private ContentTypeFilter getContentTypeFilterForIcebergTableOnly() {
    return new ContentTypeFilter() {
      @Override
      public boolean test(Content.Type type) {
        return type == Content.Type.ICEBERG_TABLE;
      }

      @Override
      public Set<Content.Type> validTypes() {
        return Set.of(Content.Type.ICEBERG_TABLE);
      }
    };
  }
}

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
package com.dremio.service.orphanage;


import com.dremio.datastore.api.Document;
import com.dremio.service.orphanage.proto.OrphanEntry;

/**
 * Orphanage is the store for storing the orphan entries
 */
public interface Orphanage {

  /**
   *Factory for Orphanage
   */
  interface Factory {

    Orphanage   get();

  }

  void deleteOrphan(OrphanEntry.OrphanId key);

  void addOrUpdateOrphan(OrphanEntry.OrphanId key, OrphanEntry.Orphan val);

  void addOrphan(OrphanEntry.Orphan val);

  Document<OrphanEntry.OrphanId, OrphanEntry.Orphan> getOrphan(OrphanEntry.OrphanId key);

  Iterable<Document<OrphanEntry.OrphanId, OrphanEntry.Orphan>> getAllOrphans();


}

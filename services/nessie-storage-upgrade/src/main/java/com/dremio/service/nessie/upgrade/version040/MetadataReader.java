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
package com.dremio.service.nessie.upgrade.version040;

/**
 * Retrieves Nessie entries from an old format so that they can be migrated to the current
 * format.
 */
public interface MetadataReader {
  /**
   * Provides each Nessie entry that needs to be migrated to the provided
   * commit consumer.
   * @param commitConsumer consumer that will save the entry in the current Nessie format
   */
  void doUpgrade(CommitConsumer commitConsumer);
}

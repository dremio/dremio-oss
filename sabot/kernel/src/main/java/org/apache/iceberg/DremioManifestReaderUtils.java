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
package org.apache.iceberg;

import org.apache.avro.Schema;
import org.apache.iceberg.io.CloseableIterable;

/** Utility functions for working with Iceberg ManifestReaders. */
public class DremioManifestReaderUtils {

  public static class ManifestEntryWrapper<F extends ContentFile<F>> {

    private ManifestEntry<F> entry;

    ManifestEntryWrapper() {}

    public ManifestEntryWrapper(F file, long sequenceNumber) {
      // TODO: add a constructor with 'file sequence number'.
      // This method is only used for test code simulation. Also, 'data sequence number' is always
      // passed as 0.
      this.entry =
          new GenericManifestEntry<F>((Schema) null)
              .wrapExisting(0L, sequenceNumber, sequenceNumber, file);
    }

    public Long sequenceNumber() {
      return entry.dataSequenceNumber();
    }

    public F file() {
      return entry.file();
    }

    public ManifestEntryWrapper<F> wrap(ManifestEntry<F> entry) {
      this.entry = entry;
      return this;
    }
  }

  /**
   * Provides public access to ManifestReader.liveEntries. Callers are responsible for making
   * defensive copies of ManifestEntryWrapper instances during iteration.
   */
  public static <F extends ContentFile<F>>
      CloseableIterable<ManifestEntryWrapper<F>> liveManifestEntriesIterator(
          ManifestReader<F> reader) {
    ManifestEntryWrapper<F> wrapper = new ManifestEntryWrapper<>();
    return CloseableIterable.transform(reader.liveEntries(), wrapper::wrap);
  }
}

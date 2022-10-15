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
package com.dremio.exec.catalog;

import java.io.IOException;
import java.util.Iterator;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.orphanage.proto.OrphanEntry;

public final class CatalogUtil {

  private CatalogUtil() {
  }

  /**
   * Save a partition chunk listing that refers to all partition chunks since the last invocation
   * of {@link #savePartitionChunksInSplitsStores(DatasetMetadataSaver, PartitionChunkListing)}, or since the creation of this metadata saver, whichever came last.
   * Also calculates to total number of records across every split and every partition chunk listed.
   *
   * @param chunkListing The partition chunks to save.
   * @return The total record count of all splits in chunkListing.
   */
  public static long savePartitionChunksInSplitsStores(DatasetMetadataSaver saver, PartitionChunkListing chunkListing) throws IOException {
    long recordCountFromSplits = 0;
    final Iterator<? extends PartitionChunk> chunks = chunkListing.iterator();
    while (chunks.hasNext()) {
      final PartitionChunk chunk = chunks.next();

      final Iterator<? extends DatasetSplit> splits = chunk.getSplits().iterator();
      while (splits.hasNext()) {
        final DatasetSplit split = splits.next();
        saver.saveDatasetSplit(split);
        recordCountFromSplits += split.getRecordCount();
      }
      saver.savePartitionChunk(chunk);
    }
    return recordCountFromSplits;
  }

  public static boolean requestedPluginSupportsVersionedTables(NamespaceKey key, Catalog catalog) {
    return requestedPluginSupportsVersionedTables(key.getRoot(), catalog);
  }

  public static boolean requestedPluginSupportsVersionedTables(String sourceName, Catalog catalog) {
    try {
      return catalog.getSource(sourceName) instanceof VersionedPlugin;
    } catch (UserException ignored) {
      // Source not found
      return false;
    }
  }

  public static ResolvedVersionContext resolveVersionContext(Catalog catalog, String sourceName, VersionContext version) {
    if (!requestedPluginSupportsVersionedTables(sourceName, catalog)) {
      return null;
    }
    try {
      return catalog.resolveVersionContext(sourceName, version);
    } catch (ReferenceNotFoundException e) {
      throw UserException.validationError(e)
        .message("Requested %s not found on source %s.", version, sourceName)
        .buildSilently();
    } catch (NoDefaultBranchException e) {
      throw UserException.validationError(e)
        .message("Unable to resolve source version. Version was not specified and Source %s does not have a default branch set.", sourceName)
        .buildSilently();
    } catch (ReferenceConflictException e) {
      throw UserException.validationError(e)
        .message("Requested %s in source %s is not the requested type.", version, sourceName)
        .buildSilently();
    }
  }

  public static boolean hasIcebergMetadata(DatasetConfig datasetConfig) {
    if (datasetConfig.getPhysicalDataset() != null) {
      if (datasetConfig.getPhysicalDataset().getIcebergMetadataEnabled() != null &&
        datasetConfig.getPhysicalDataset().getIcebergMetadataEnabled() &&
        datasetConfig.getPhysicalDataset().getIcebergMetadata() != null) {
        return true;
      }
    }
    return false;
  }

  public static OrphanEntry.Orphan createIcebergMetadataOrphan(DatasetConfig datasetConfig) {
    String tableUuid = datasetConfig.getPhysicalDataset().getIcebergMetadata().getTableUuid();
    OrphanEntry.OrphanIcebergMetadata icebergOrphan = OrphanEntry.OrphanIcebergMetadata.newBuilder().setIcebergTableUuid(tableUuid).setDatasetTag(datasetConfig.getTag()).addAllDatasetFullPath(datasetConfig.getFullPathList()).build();
    long currTime = System.currentTimeMillis();
    return OrphanEntry.Orphan.newBuilder().setOrphanType(OrphanEntry.OrphanType.ICEBERG_METADATA).setCreatedAt(currTime).setScheduledAt(currTime).setOrphanDetails(icebergOrphan.toByteString()).build();
  }

  public static void addIcebergMetadataOrphan(DatasetConfig datasetConfig, Orphanage orphanage) {

    if (hasIcebergMetadata(datasetConfig)) {
      OrphanEntry.Orphan orphanEntry = createIcebergMetadataOrphan(datasetConfig);
      addIcebergMetadataOrphan(orphanEntry, orphanage);
    }

  }

  public static void addIcebergMetadataOrphan(OrphanEntry.Orphan orphanEntry, Orphanage orphanage) {
    orphanage.addOrphan(orphanEntry);
  }


  public static NamespaceService.DeleteCallback getDeleteCallback(Orphanage orphanage) {
    NamespaceService.DeleteCallback deleteCallback = (DatasetConfig datasetConfig) -> {
      addIcebergMetadataOrphan(datasetConfig, orphanage);
    };
    return deleteCallback;
  }

  public static void validateResolvedVersionIsBranch(ResolvedVersionContext resolvedVersionContext, String tableName) {
    if ((resolvedVersionContext != null) && !resolvedVersionContext.isBranch()) {
      throw UserException.validationError()
        .message("Unable to perform operation on %s - version %s is not a branch ",
          tableName,
          resolvedVersionContext.getRefName())
        .buildSilently();
    }
  }

  public static boolean isFSInternalIcebergTableOrJsonTableOrMongo(Catalog catalog, NamespaceKey path, DatasetConfig dataset) {
    StoragePlugin storagePlugin;
    try {
      storagePlugin = catalog.getSource(path.getRoot());
    } catch (UserException uex) {
      throw UserException.validationError()
        .message("Source [%s] not found", path)
        .buildSilently();
    }

    if (!(storagePlugin instanceof MutablePlugin)) {
      return false;
    }

    return ((MutablePlugin) storagePlugin).isSupportUserDefinedSchema(dataset);

}

  /**
   * Utility to return TimeTravelRequest for query : select * from iceberg_table AT SNAPSHOT/TIMESTAMP
   * @param key
   * @param context
   * @return
   */
  public static TimeTravelOption.TimeTravelRequest getTimeTravelRequest(NamespaceKey key, TableVersionContext context) {
    switch (context.getType()) {
      case SNAPSHOT_ID:
        return TimeTravelOption.newSnapshotIdRequest(context.getValueAs(String.class));
      case TIMESTAMP:
        final long millis = context.getValueAs(Long.class);
        if (millis > System.currentTimeMillis()) {
          throw UserException.validationError()
            .message("For table '%s', the provided time travel timestamp value '%d' is out of range",
              key.getPathComponents(), millis)
            .buildSilently();
        }
        return TimeTravelOption.newTimestampRequest(millis);
      case LATEST_VERSION:
      case BRANCH:
      case TAG:
      case COMMIT_HASH_ONLY:
      case REFERENCE:
        return null;
      default:
        throw new AssertionError("Unsupported type " + context.getType());
    }
  }
}

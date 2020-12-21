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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.NamespaceInternalKeyDumpUtil.parseKey;
import static com.dremio.service.namespace.NamespaceUtils.getId;
import static com.dremio.service.namespace.NamespaceUtils.isListable;
import static com.dremio.service.namespace.NamespaceUtils.isPhysicalDataset;
import static com.dremio.service.namespace.NamespaceUtils.lastElement;
import static com.dremio.service.namespace.NamespaceUtils.setId;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.DATASET;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.FOLDER;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.HOME;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SOURCE;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SPACE;
import static java.lang.String.format;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.xerial.snappy.SnappyOutputStream;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStore.LegacyFindByRange;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.MultiSplit;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;

/**
 * Namespace management.
 */
public class NamespaceServiceImpl implements NamespaceService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NamespaceServiceImpl.class);

  public static final String DAC_NAMESPACE = "dac-namespace";
  // NOTE: the name of the partition chunks store needs to stay "metadata-dataset-splits" for backwards compatibility.
  public static final String PARTITION_CHUNKS = "metadata-dataset-splits";
  public static final String MULTI_SPLITS = "metadata-multi-splits";

  private final LegacyIndexedStore<String, NameSpaceContainer> namespace;
  private final LegacyIndexedStore<PartitionChunkId, PartitionChunk> partitionChunkStore;
  private final LegacyKVStore<PartitionChunkId, MultiSplit> multiSplitStore;
  private final boolean keyNormalization;

  /**
   * Factory for {@code NamespaceServiceImpl}
   */
  public static final class Factory implements NamespaceService.Factory {
    private final LegacyKVStoreProvider kvStoreProvider;

    @Inject
    public Factory(LegacyKVStoreProvider kvStoreProvider) {
      this.kvStoreProvider = kvStoreProvider;
    }

    @Override
    public NamespaceService get(String userName) {
      Preconditions.checkNotNull(userName, "requires userName"); // per method contract
      return new NamespaceServiceImpl(kvStoreProvider);
    }
  }

  @Inject
  public NamespaceServiceImpl(final LegacyKVStoreProvider kvStoreProvider) {
    this(kvStoreProvider, true);
  }

  protected NamespaceServiceImpl(final LegacyKVStoreProvider kvStoreProvider, boolean keyNormalization) {
    this.namespace = kvStoreProvider.getStore(NamespaceStoreCreator.class);
    this.partitionChunkStore = kvStoreProvider.getStore(PartitionChunkCreator.class);
    this.multiSplitStore = kvStoreProvider.getStore(MultiSplitStoreCreator.class);
    this.keyNormalization = keyNormalization;
  }

  /**
   * Creator for name space kvstore.
   */
  public static class NamespaceStoreCreator implements LegacyIndexedStoreCreationFunction<String, NameSpaceContainer> {

    @Override
    public LegacyIndexedStore<String, NameSpaceContainer> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, NameSpaceContainer>newStore()
        .name(DAC_NAMESPACE)
        .keyFormat(Format.ofString())
        .valueFormat(Format.wrapped(
          NameSpaceContainer.class,
          NameSpaceContainer::toProtoStuff,
          NameSpaceContainer::new,
          Format.ofProtostuff(com.dremio.service.namespace.protostuff.NameSpaceContainer.class)))
        .versionExtractor(NameSpaceContainerVersionExtractor.class)
        .buildIndexed(new NamespaceConverter());
    }

  }

  @SuppressWarnings("unchecked")
  private static final Format<PartitionChunkId> PARTITION_CHUNK_ID_FORMAT =
    Format.wrapped(PartitionChunkId.class, PartitionChunkId::getSplitId, PartitionChunkId::of, Format.ofString());

  /**
   * KVStore creator for partition chunks table
   */
  public static class PartitionChunkCreator implements LegacyIndexedStoreCreationFunction<PartitionChunkId, PartitionChunk> {

    @SuppressWarnings("unchecked")
    @Override
    public LegacyIndexedStore<PartitionChunkId, PartitionChunk> build(LegacyStoreBuildingFactory factory) {
      return factory.<PartitionChunkId, PartitionChunk>newStore()
        .name(PARTITION_CHUNKS)
        .keyFormat(PARTITION_CHUNK_ID_FORMAT)
        .valueFormat(Format.ofProtobuf(PartitionChunk.class))
        .buildIndexed(new PartitionChunkConverter());
    }
  }

  /**
   * Comparator for partition chunk ranges
   */
  private static final Comparator<Range<PartitionChunkId>> PARTITION_CHUNK_RANGE_COMPARATOR =
      Comparator.comparing(Range::lowerEndpoint);

  /**
   * KVStore creator for multisplits table
   */
  public static class MultiSplitStoreCreator implements LegacyKVStoreCreationFunction<PartitionChunkId, MultiSplit> {

    @Override
    public LegacyKVStore<PartitionChunkId, MultiSplit> build(LegacyStoreBuildingFactory factory) {
      return factory.<PartitionChunkId, MultiSplit>newStore()
        .name(MULTI_SPLITS)
        .keyFormat(PARTITION_CHUNK_ID_FORMAT)
        .valueFormat(Format.ofProtobuf(MultiSplit.class))
        .build();
    }
  }

  @Override
  public int deleteSplitOrphans(PartitionChunkId.SplitOrphansRetentionPolicy policy) {
    final Map<String, SourceConfig> sourceConfigs = new HashMap<>();
    final List<Range<PartitionChunkId>> ranges = new ArrayList<>();

    // Iterate over all entries in the namespace to collect source
    // and datasets with splits to create a map of the valid split ranges.
    final Set<String> missingSources = new HashSet<>();
    for(Map.Entry<String, NameSpaceContainer> entry : namespace.find()) {
      NameSpaceContainer container = entry.getValue();
      switch(container.getType()) {
      case SOURCE: {
        final SourceConfig source = container.getSource();
        // Normalize source name because no guarantee that the dataset source path will use same capitalization
        final String sourceName = source.getName().toLowerCase(Locale.ROOT);
        final SourceConfig existing = sourceConfigs.putIfAbsent(sourceName, source);
        if (existing != null) {
          logger.warn("Two different sources with different names under the same key: {} and {}. This might cause metadata issues.", existing.getName(), source.getName());
        }
        continue;
      }

      case DATASET:
        final DatasetConfig dataset = container.getDataset();
        final ReadDefinition readDefinition = dataset.getReadDefinition();
        if (readDefinition == null || readDefinition.getSplitVersion() == null) {
          continue;
        }

        // Create a range from now - expiration to future...
        // Note that because entries are ordered, source should have been visited before the dataset
        final MetadataPolicy metadataPolicy;
        switch (dataset.getType()) {
        case PHYSICAL_DATASET_HOME_FILE:
        case PHYSICAL_DATASET_HOME_FOLDER:
          // Home dataset. Home doesn't have a configurable metadata policy/never refreshes
          metadataPolicy = null;
          break;

        case PHYSICAL_DATASET:
        case PHYSICAL_DATASET_SOURCE_FILE:
        case PHYSICAL_DATASET_SOURCE_FOLDER:
          final String sourceName = dataset.getFullPathList().get(0).toLowerCase(Locale.ROOT);
          SourceConfig source = sourceConfigs.get(sourceName);
          if (source == null) {
            if (missingSources.add(sourceName)) {
              logger.info("Source {} not found for dataset {}. Considering it as orphaned", sourceName, PathUtils.constructFullPath(dataset.getFullPathList()));
            }
            continue;
          }
          metadataPolicy = source.getMetadataPolicy();
          break;

        case VIRTUAL_DATASET:
          // Virtual datasets don't have splits
          continue;

        default:
          logger.error("Unknown dataset type {}. Cannot check for orphan splits", dataset.getType());
          return 0;
        }

        Range<PartitionChunkId> versionRange = policy.apply(metadataPolicy, dataset);
        // min version is based on when dataset definition would expire
        // but it has to be at least positive
        ranges.add(versionRange);

        continue;

      default:
        continue;
      }
    }

    // Ranges need to be sorted for binary search to be working
    Collections.sort(ranges, PARTITION_CHUNK_RANGE_COMPARATOR);

    // Some explanations:
    // ranges is setup to contain the current (exclusive) range of partition chunks for each dataset.
    // The function then iterates over all partition chunks present in the partition chunks store and verify
    // that the partition chunk belongs to one of the dataset partition chunk ranges. If not, the item is dropped.
    // The binary search provides the index in ranges where a partition chunk would be inserted if not
    // already present in the list (this is the insertion point, see Collections.binarySort
    // javadoc), which should be just after the corresponding dataset range as ranges items
    // are sorted based on their lower endpoint.
    int elementCount = 0;
    for (Map.Entry<PartitionChunkId, PartitionChunk> e : partitionChunkStore.find()) {
      PartitionChunkId id = e.getKey();
      final int item = Collections.binarySearch(ranges, Range.singleton(id), PARTITION_CHUNK_RANGE_COMPARATOR);

      // we should never find a match since we're searching for a split key and that dataset
      // split range endpoints are excluded/not valid split keys
      Preconditions.checkState(item < 0);

      final int insertionPoint = (-item) - 1;
      final int consideredRange = insertionPoint - 1; // since a normal match would come directly after the start range, we need to check the range directly above the insertion point.

      if (consideredRange < 0 || !ranges.get(consideredRange).contains(id)) {
        logger.debug("Deleting partition chunk associated with key {} from the partition chunk store.", e.getKey());
        partitionChunkStore.delete(e.getKey());
        ++elementCount;
      }
    }

    for (Map.Entry<PartitionChunkId, MultiSplit> e : multiSplitStore.find()) {
      PartitionChunkId id = e.getKey();
      final int item = Collections.binarySearch(ranges, Range.singleton(id), PARTITION_CHUNK_RANGE_COMPARATOR);

      // we should never find a match since we're searching for a split key and that dataset
      // split range endpoints are excluded/not valid split keys
      Preconditions.checkState(item < 0);

      final int insertionPoint = (-item) - 1;
      final int consideredRange = insertionPoint - 1; // since a normal match would come directly after the start range, we need to check the range directly above the insertion point.

      if (consideredRange < 0 || !ranges.get(consideredRange).contains(id)) {
        logger.debug("Deleting multi split associated with key {} from the multi split store.", e.getKey());
        multiSplitStore.delete(e.getKey());
      }
    }

    return elementCount;
  }

  protected LegacyIndexedStore<String, NameSpaceContainer> getStore() {
    return namespace;
  }

  /**
   * Helper method which creates a new entity or update the existing entity with given entity
   *
   * @param entity
   * @throws NamespaceException
   */
  private void createOrUpdateEntity(final NamespaceEntity entity, NamespaceAttribute... attributes) throws NamespaceException {
    final NamespaceKey entityPath = entity.getPathKey().getPath();

    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(entityPath);
    doCreateOrUpdateEntity(entity, entitiesOnPath, attributes);
  }

  protected void doCreateOrUpdateEntity(final NamespaceEntity entity, List<NameSpaceContainer> entitiesOnPath, NamespaceAttribute... attributes) throws NamespaceException {
    final NameSpaceContainer prevContainer = lastElement(entitiesOnPath);
    ensureIdExistsTypeMatches(entity, prevContainer);

    namespace.put(entity.getPathKey().getKey(), entity.getContainer());
  }

  /**
   * Checks if the updated entity matches the type of the existing container.  In the case of promoting a folder to a
   * dataset, it will delete the existing folder namespace entry (which is created when you un-promote a folder dataset).
   *
   * @param newOrUpdatedEntity
   * @param existingContainer
   * @return false if there was a side effect where existingContainer was deleted
   * @throws NamespaceException
   */
  // TODO: Remove this operation and move to kvstore
  protected boolean ensureIdExistsTypeMatches(NamespaceEntity newOrUpdatedEntity, NameSpaceContainer existingContainer) throws NamespaceException{
    final String idInContainer = getId(newOrUpdatedEntity.getContainer());

    if (existingContainer == null) {
      if (idInContainer != null) {
        // If the client already gives an id, take it.
        return true;
      }

      // generate a new id
      setId(newOrUpdatedEntity.getContainer(), UUID.randomUUID().toString());
      return true;
    }

    // TODO: this is a side effect and shouldn't be done in this method.
    // Folder in source can be marked as a dataset. (folder->dataset)
    // When user removes format settings on such folder, we convert physical dataset back to folder. (dataset->folder)
    // In the case when folder is converted back into dataset, skip type checking and delete folder first and then add dataset.
    if (newOrUpdatedEntity.getContainer().getType() == DATASET &&
      isPhysicalDataset(newOrUpdatedEntity.getContainer().getDataset().getType()) &&
      existingContainer.getType() == FOLDER) {
      namespace.delete((new NamespaceInternalKey(new NamespaceKey(existingContainer.getFullPathList()), keyNormalization)).getKey(),
        existingContainer.getFolder().getTag());
      return false;
    }

    NameSpaceContainerVersionExtractor extractor = new NameSpaceContainerVersionExtractor();
    // let's make sure we aren't dealing with a concurrent update before we try to check other
    // conditions. A concurrent update should throw that exception rather than the user exceptions
    // thrown later in this check. Note, this duplicates the version check operation that is done
    // inside the kvstore.
    final String newVersion = extractor.getTag(newOrUpdatedEntity.getContainer());
    final String oldVersion = extractor.getTag(existingContainer);
    if(!Objects.equals(newVersion, oldVersion)) {
      final String expectedAction = newVersion == null ? "create" : "update version " + newVersion;
      final String previousValueDesc = oldVersion == null ? "no previous version" : "previous version " + oldVersion;
      throw new ConcurrentModificationException(format("tried to %s, found %s", expectedAction, previousValueDesc));
    }

    if (
      // make sure the type of the existing entity and new entity are the same
      newOrUpdatedEntity.getContainer().getType() != existingContainer.getType() ||
        // If the id in new container is null, then it must be that the user is trying to create another entry of same type at the same path.
        idInContainer == null) {
      throw UserException.validationError()
        .message("There already exists an entity of type [%s] at given path [%s]",
          existingContainer.getType(), newOrUpdatedEntity.getPathKey().getPath())
        .build(logger);
    }

    // make sure the id remains the same
    final String idInExistingContainer = getId(existingContainer);
    if (!idInExistingContainer.equals(idInContainer)) {
      throw UserException.invalidMetadataError().message("There already exists an entity of type [%s] at given path [%s] with Id %s. Unable to replace with Id %s",
          existingContainer.getType(), newOrUpdatedEntity.getPathKey().getPath(), idInExistingContainer, idInContainer).buildSilently();
    }

    return true;
  }

  @Override
  public void addOrUpdateSource(NamespaceKey sourcePath, SourceConfig sourceConfig, NamespaceAttribute... attributes) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(SOURCE, sourcePath, sourceConfig, keyNormalization, new ArrayList<>()), attributes);
  }

  @Override
  public void addOrUpdateSpace(NamespaceKey spacePath, SpaceConfig spaceConfig, NamespaceAttribute... attributes) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(SPACE, spacePath, spaceConfig, keyNormalization, new ArrayList<>()), attributes);
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset, NamespaceAttribute... attributes) throws NamespaceException {
    dataset.setSchemaVersion(DatasetHelper.CURRENT_VERSION);

    if (dataset.getVersion() == null && dataset.getCreatedAt() == null) {
      dataset.setCreatedAt(System.currentTimeMillis());
    }

    dataset.setLastModified(System.currentTimeMillis());

    // ensure physical dataset has acceleration TTL
    final DatasetType type = dataset.getType();
    switch (type) {
      case PHYSICAL_DATASET:
      case PHYSICAL_DATASET_SOURCE_FILE:
      case PHYSICAL_DATASET_SOURCE_FOLDER: {
        createSourceFolders(datasetPath);
        if(dataset.getPhysicalDataset() == null){
          dataset.setPhysicalDataset(new PhysicalDataset());
        }
      }
      break;
      case PHYSICAL_DATASET_HOME_FILE:
      case PHYSICAL_DATASET_HOME_FOLDER: {
        if(dataset.getPhysicalDataset() == null){
          dataset.setPhysicalDataset(new PhysicalDataset());
        }
      }
      break;
      default:
        break;
    }

    createOrUpdateEntity(NamespaceEntity.toEntity(DATASET, datasetPath, dataset, keyNormalization, new ArrayList<>()), attributes);
  }

  @Override
  public boolean hasChildren(NamespaceKey key) {
    final Iterable<NameSpaceContainer> children;
    try {
      children = iterateEntity(key);
    } catch (NamespaceException e) {
      throw new RuntimeException("failed during dataset listing of sub-tree under: " + key);
    }
    if (FluentIterable.from(children).size() > 0) {
      return true;
    }
    return false;
  }

  /**
   * Accumulate metadata, then save it in the K/V store.
   */
  private class DatasetMetadataSaverImpl implements DatasetMetadataSaver {
    private final NamespaceKey datasetPath;
    private final EntityId datasetId;
    private final long nextDatasetVersion;
    private final SplitCompression splitCompression;
    private final long maxSinglePartitionChunks;
    private boolean isClosed;
    private long partitionChunkCount;
    private long partitionChunkWithSingleSplitCount;
    private List<PartitionChunkId> createdPartitionChunks;
    private long accumulatedSizeInBytes;
    private long accumulatedRecordCount;
    private List<DatasetSplit> accumulatedSplits;
    private int totalNumSplits;

    DatasetMetadataSaverImpl(NamespaceKey datasetPath, EntityId datasetId, long nextDatasetVersion, SplitCompression splitCompression, long maxSinglePartitionChunks) {
      this.datasetPath = datasetPath;
      this.datasetId = datasetId;
      this.nextDatasetVersion = nextDatasetVersion;
      this.splitCompression = splitCompression;
      this.isClosed = false;
      this.partitionChunkCount = -1;  // incremented to 0 below
      this.createdPartitionChunks = new ArrayList<>();
      this.totalNumSplits = 0;
      this.partitionChunkWithSingleSplitCount = 0;
      this.maxSinglePartitionChunks = maxSinglePartitionChunks;
      resetSplitAccumulation();
    }

    private boolean isSingleSplitPartitionAllowed() {
      return partitionChunkWithSingleSplitCount < maxSinglePartitionChunks;
    }

    private void resetSplitAccumulation() {
      ++partitionChunkCount;
      accumulatedRecordCount = 0;
      accumulatedSizeInBytes = 0;
      accumulatedSplits = new ArrayList<>();
    }

    private void saveDatasetSplit(DatasetSplit split) {
      if (isClosed) {
        throw new IllegalStateException("Attempting to save a dataset split after the saver was closed");
      }
      accumulatedRecordCount += split.getRecordCount();
      accumulatedSizeInBytes += split.getSizeInBytes();
      accumulatedSplits.add(split);
    }

    private void savePartitionChunk(com.dremio.connector.metadata.PartitionChunk partitionChunk) throws IOException {
      Preconditions.checkState(!isClosed, "Attempting to save a partition chunk after the saver was closed");
      if (accumulatedSplits.isEmpty()) {
        // Must have at least one split for the partition chunk
        throw new IllegalStateException("Must save at least one split before saving a partition chunk");
      }
      String splitKey = Long.toString(partitionChunkCount);
      PartitionChunk.Builder builder = PartitionChunk.newBuilder()
          .setSize(accumulatedSizeInBytes)
          .setRowCount(accumulatedRecordCount)
          .setPartitionExtendedProperty(MetadataProtoUtils.toProtobuf(partitionChunk.getExtraInfo()))
          .addAllPartitionValues(partitionChunk.getPartitionValues().stream()
              .map(MetadataProtoUtils::toProtobuf)
              .collect(Collectors.toList()))
          .setSplitKey(splitKey)
          .setSplitCount(accumulatedSplits.size());
      // Only a limited number of partition chunks are allowed to be saved with a single dataset split.
      // Once it reaches this limit(=maxSinglePartitionChunks), save splits separately in multi-split store.
      final boolean singleSplitPartitionAllowed = isSingleSplitPartitionAllowed();
      if (accumulatedSplits.size() == 1 && singleSplitPartitionAllowed) {
        // Single-split partition chunk
        builder.setDatasetSplit(MetadataProtoUtils.toProtobuf(accumulatedSplits.get(0)));
        partitionChunkWithSingleSplitCount++;
      }
      PartitionChunkId chunkId = PartitionChunkId.of(datasetId, nextDatasetVersion, splitKey);
      NamespaceServiceImpl.this.partitionChunkStore.put(chunkId, builder.build());
      createdPartitionChunks.add(chunkId);
      // Intentionally creating any potential multi-splits after creating the partition chunk.
      // This makes orphan cleaning simpler, as we can key only on the existing partitionChunk(s), and remove
      // any matching multi-splits
      if (accumulatedSplits.size() > 1 || !singleSplitPartitionAllowed) {
        NamespaceServiceImpl.this.multiSplitStore.put(chunkId, createMultiSplitFromAccumulated(splitKey));
      }
      totalNumSplits += accumulatedSplits.size();
      resetSplitAccumulation();
    }

    private OutputStream wrapIfNeeded(OutputStream o) throws IOException {
      switch (splitCompression) {
        case UNCOMPRESSED:
          return o;
        case SNAPPY:
          return new SnappyOutputStream(o);
        default:
          throw new IllegalArgumentException(String.format("Unsupported compression type: %s", splitCompression));
      }
    }

    private MultiSplit.Codec getCodecType() {
      switch (splitCompression) {
        case UNCOMPRESSED:
          return MultiSplit.Codec.UNCOMPRESSED;
        case SNAPPY:
          return MultiSplit.Codec.SNAPPY;
        default:
          throw new IllegalArgumentException(String.format("Unsupported compression type: %s", splitCompression));
      }
    }

    /**
     * Create a MultiSplit from the accumulated splits
     * @return
     */
    private MultiSplit createMultiSplitFromAccumulated(String splitKey) throws IOException {
      ByteString.Output output = ByteString.newOutput();
      OutputStream wrappedOutput = wrapIfNeeded(output);
      for (DatasetSplit split: accumulatedSplits) {
        MetadataProtoUtils.toProtobuf(split).writeDelimitedTo(wrappedOutput);
      }
      wrappedOutput.flush();
      ByteString splitData = output.toByteString();
      return MultiSplit.newBuilder()
        .setMultiSplitKey(splitKey)
        .setCodec(getCodecType())
        .setSplitCount(accumulatedSplits.size())
        .setSplitData(splitData)
        .build();
    }

    @Override
    public long savePartitionChunks(PartitionChunkListing chunkListing) throws IOException {
      final Iterator<? extends com.dremio.connector.metadata.PartitionChunk> chunks = chunkListing.iterator();
      long recordCountFromSplits = 0;
      while (chunks.hasNext()) {
        final com.dremio.connector.metadata.PartitionChunk chunk = chunks.next();

        final Iterator<? extends DatasetSplit> splits = chunk.getSplits().iterator();
        while (splits.hasNext()) {
          final DatasetSplit split = splits.next();
          saveDatasetSplit(split);
          recordCountFromSplits += split.getRecordCount();
        }
        savePartitionChunk(chunk);
      }
      return recordCountFromSplits;
    }

    @Override
    public void saveDataset(DatasetConfig datasetConfig, boolean opportunisticSave, NamespaceAttribute... attributes) throws NamespaceException {
      Preconditions.checkState(!isClosed, "Attempting to save a partition chunk after the whole dataset was saved");
      Objects.requireNonNull(datasetConfig.getId(), "ID is required");
      Objects.requireNonNull(datasetConfig.getReadDefinition(), "read_definition is required");
      datasetConfig.getReadDefinition().setSplitVersion(nextDatasetVersion);
      datasetConfig.setTotalNumSplits(totalNumSplits);
      while (true) {
        try {
          NamespaceServiceImpl.this.addOrUpdateDataset(datasetPath, datasetConfig, attributes);
          isClosed = true;
          break;
        } catch (ConcurrentModificationException cme) {
          if (opportunisticSave) {
            throw cme;
          }
          // Get dataset config again
          final DatasetConfig existingDatasetConfig = NamespaceServiceImpl.this.getDataset(datasetPath);
          if (existingDatasetConfig.getReadDefinition() != null &&
            existingDatasetConfig.getReadDefinition().getSplitVersion() != null &&
            // Only delete splits if strictly newer. If splitVersions are equals, we
            // could end up delete the splits of the existing dataset (see DX-12232)
            existingDatasetConfig.getReadDefinition().getSplitVersion() > nextDatasetVersion) {
            deleteSplits(createdPartitionChunks);
            // copy splitVersion and other details from existingConfig
            datasetConfig.getReadDefinition().setSplitVersion(existingDatasetConfig.getReadDefinition().getSplitVersion());
            datasetConfig.setTotalNumSplits(existingDatasetConfig.getTotalNumSplits());
            datasetConfig.setTag(existingDatasetConfig.getTag());
            isClosed = true;
            break;
          }
          // try again if read definition is not set or splits are not up-to-date.
          datasetConfig.setTag(existingDatasetConfig.getTag());
        }
      }
    }

    @Override
    public void close() {
      if (!isClosed) {
        deleteSplits(createdPartitionChunks);
      }
    }
  }

  @Override
  public DatasetMetadataSaver newDatasetMetadataSaver(NamespaceKey datasetPath, EntityId datasetId, SplitCompression splitCompression, long maxSinglePartitionChunks) {
    return new DatasetMetadataSaverImpl(datasetPath, datasetId, System.currentTimeMillis(), splitCompression, maxSinglePartitionChunks);
  }

  /**
   * Return true if new splits are not same as old splits
   * @param dataset dataset config
   * @param newSplits new splits to be added
   * @param oldSplits existing old splits
   * @return false if old and new splits are same
   */
  private static boolean compareSplits(DatasetConfig dataset,
                                       List<PartitionChunk> newSplits,
                                       Iterable<PartitionChunk> oldSplits) {
    // if both old and new splits are null return false
    if (oldSplits == null && newSplits == null) {
      return false;
    }
    // if new splits are not null, but old splits, not the same...
    if (oldSplits == null) {
      return true;
    }

    final Map<PartitionChunkId, PartitionChunk> newSplitsMap = new HashMap<>();
    for (PartitionChunk newSplit: newSplits) {
      final PartitionChunkId splitId = PartitionChunkId.of(dataset, newSplit, dataset.getReadDefinition().getSplitVersion());
      newSplitsMap.put(splitId, newSplit);
    }

    int oldSplitsCount = 0;
    for (PartitionChunk partitionChunk : oldSplits) {
      final PartitionChunkId splitId = PartitionChunkId.of(dataset, partitionChunk, dataset.getReadDefinition().getSplitVersion());
      final PartitionChunk newSplit = newSplitsMap.get(splitId);
      if (newSplit == null || !newSplit.equals(partitionChunk)) {
        return true;
      }
      ++oldSplitsCount;
    }
    return newSplits.size() != oldSplitsCount;
  }

  private static Iterable<PartitionChunk> partitionChunkValues(Iterable<Map.Entry<PartitionChunkId, PartitionChunk>> partitionChunks) {
    return FluentIterable
      .from(partitionChunks)
      .transform(Entry::getValue);
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset, List<PartitionChunk> splits, NamespaceAttribute... attributes) throws NamespaceException {
    Preconditions.checkNotNull(dataset.getReadDefinition());

    if (dataset.getReadDefinition() != null && dataset.getReadDefinition().getSplitVersion() != null &&
      !compareSplits(dataset, splits, partitionChunkValues(partitionChunkStore.find(PartitionChunkId.getSplitsRange(dataset))))) {
      addOrUpdateDataset(datasetPath, dataset, attributes);
      return;
    }

    if (dataset.getId() == null) {
      dataset.setId(new EntityId(UUID.randomUUID().toString())); // TODO: is this a hack?
    }

    final long nextSplitVersion = System.currentTimeMillis();
    final List<PartitionChunkId> splitIds = Lists.newArrayList();
    // only if splits have changed update splits version and retry read definition on concurrent modification.
    for (PartitionChunk split : splits) {
      final PartitionChunkId splitId = PartitionChunkId.of(dataset, split, nextSplitVersion);
      partitionChunkStore.put(splitId, split);
      splitIds.add(splitId);
    }
    dataset.getReadDefinition().setSplitVersion(nextSplitVersion);
    while (true) {
      try {
        addOrUpdateDataset(datasetPath, dataset, attributes);
        break;
      } catch (ConcurrentModificationException cme) {
        // Get dataset config again
        final DatasetConfig existingDatasetConfig = getDataset(datasetPath);
        if (existingDatasetConfig.getReadDefinition() != null &&
          existingDatasetConfig.getReadDefinition().getSplitVersion() != null &&
          // Only delete splits if strictly newer. If splitVersions are equals, we
          // could end up delete the splits of the existing dataset (see DX-12232)
          existingDatasetConfig.getReadDefinition().getSplitVersion() > nextSplitVersion) {
          deleteSplits(splitIds);
          break;
        }
        // try again if read definition is not set or splits are not up-to-date.
        dataset.setTag(existingDatasetConfig.getTag());
      }
    }
  }

  @VisibleForTesting
  void directInsertLegacySplit(DatasetConfig dataset, PartitionChunk split, long splitVersion) {
    final PartitionChunkId splitId = PartitionChunkId.of(dataset, split, splitVersion);
    partitionChunkStore.put(splitId, split);
  }

  @Override
  public void deleteSplits(Iterable<PartitionChunkId> splits) {
    for (PartitionChunkId split: splits) {
      partitionChunkStore.delete(split);
      multiSplitStore.delete(split);
    }
  }

  @Override
  public void addOrUpdateFolder(NamespaceKey folderPath, FolderConfig folderConfig,  NamespaceAttribute... attributes) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(FOLDER, folderPath, folderConfig, keyNormalization, new ArrayList<>()), attributes);
  }

  @Override
  public void addOrUpdateHome(NamespaceKey homePath, HomeConfig homeConfig) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(HOME, homePath, homeConfig, keyNormalization, new ArrayList<>()));
  }

  @Override
  public List<NameSpaceContainer> getEntities(List<NamespaceKey> lookupKeys) throws NamespaceException {
    return doGetEntities(lookupKeys);
  }

  @Override
  public DatasetConfig findDatasetByUUID(String uuid) {
    NameSpaceContainer namespaceContainer = getByIndex(DatasetIndexKeys.DATASET_UUID, uuid);
    return (namespaceContainer!=null)?namespaceContainer.getDataset():null;
  }

  @Override
  public void canSourceConfigBeSaved(SourceConfig newConfig, SourceConfig existingConfig, NamespaceAttribute... attributes) throws ConcurrentModificationException, NamespaceException {
    if (!Objects.equals(newConfig.getTag(), existingConfig.getTag())) {
      throw new ConcurrentModificationException(
        String.format("Source [%s] has been updated, and the given configuration is out of date (current tag: %s, given: %s)",
          existingConfig.getName(), existingConfig.getTag(), newConfig.getTag()));
    }
  }

  @Override
  public String getEntityIdByPath(NamespaceKey datasetPath) throws NamespaceException {
    final List<NameSpaceContainer> entities = getEntities(Arrays.asList(datasetPath));
    NameSpaceContainer entity = entities.get(0);

    return entity != null ? NamespaceUtils.getId(entity) : null;
  }

  protected List<NameSpaceContainer> doGetEntities(List<NamespaceKey> lookupKeys) {
    final List<String> keys = FluentIterable.from(lookupKeys).transform(new Function<NamespaceKey, String>() {
      @Override
      public String apply(NamespaceKey input) {
        return new NamespaceInternalKey(input, keyNormalization).getKey();
      }
    }).toList();

    return namespace.get(keys);
  }

  // GET

  @Override
  public boolean exists(final NamespaceKey key, final Type type) {
    final NameSpaceContainer container = namespace.get(new NamespaceInternalKey(key, keyNormalization).getKey());
    return container != null && container.getType() == type;
  }

  @Override
  public boolean exists(final NamespaceKey key) {
    final NameSpaceContainer container = namespace.get(new NamespaceInternalKey(key).getKey());
    return container != null;
  }

  /**
   * Helper method which retrieves the entity with given key. No authorization check done.
   *
   * @param key
   * @return
   * @throws NamespaceException
   */
  private NameSpaceContainer getEntity(final NamespaceKey key, Type type)
    throws NamespaceException {
    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(key);
    NameSpaceContainer container = lastElement(entitiesOnPath);
    if (container == null || container.getType() != type) {
      throw new NamespaceNotFoundException(key, "not found");
    }

    return doGetEntity(key, type, entitiesOnPath);
  }

  protected NameSpaceContainer doGetEntity(final NamespaceKey key, Type type, List<NameSpaceContainer> entitiesOnPath) throws NamespaceException {
    NameSpaceContainer container = lastElement(entitiesOnPath);

    return container;

  }

  protected NameSpaceContainer getEntityByIndex(IndexKey key, String index, Type type) throws NamespaceException {
    NameSpaceContainer namespaceContainer = getByIndex(key, index);

    if (namespaceContainer == null || namespaceContainer.getType() != type) {
      throw new NamespaceNotFoundException(new NamespaceKey(key.toString()), "not found");
    }

    return namespaceContainer;
  }

  @Override
  public SourceConfig getSource(NamespaceKey sourcePath) throws NamespaceException {
    return getEntity(sourcePath, SOURCE).getSource();
  }

  @Override
  public SourceConfig getSourceById(String id) throws NamespaceException {
    return getEntityByIndex(NamespaceIndexKeys.SOURCE_ID, id, SOURCE).getSource();
  }

  @Override
  public SpaceConfig getSpace(NamespaceKey spacePath) throws NamespaceException {
    return getEntity(spacePath, SPACE).getSpace();
  }

  @Override
  public SpaceConfig getSpaceById(String id) throws NamespaceException {
    return getEntityByIndex(NamespaceIndexKeys.SPACE_ID, id, SPACE).getSpace();
  }

  @Override
  public NameSpaceContainer getEntityById(String id) throws NamespaceException {
    final SearchQuery query = SearchQueryUtils.or(
      SearchQueryUtils.newTermQuery(DatasetIndexKeys.DATASET_UUID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.SOURCE_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.SPACE_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.HOME_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.FOLDER_ID, id));

    final LegacyFindByCondition condition = new LegacyFindByCondition()
      .setOffset(0)
      .setLimit(1)
      .setCondition(query);

    final Iterable<Entry<String, NameSpaceContainer>> result = namespace.find(condition);
    final Iterator<Entry<String, NameSpaceContainer>> it = result.iterator();
    return it.hasNext()?it.next().getValue():null;
  }

  @Override
  public DatasetConfig getDataset(NamespaceKey datasetPath) throws NamespaceException {
    return getEntity(datasetPath, DATASET).getDataset();
  }

  @Override
  public FolderConfig getFolder(NamespaceKey folderPath) throws NamespaceException {
    return getEntity(folderPath, FOLDER).getFolder();
  }

  @Override
  public HomeConfig getHome(NamespaceKey homePath) throws NamespaceException {
    return getEntity(homePath, HOME).getHome();
  }

  /**
   * Helper method that returns all root level entities of given type.
   *
   * @return
   */
  protected List<NameSpaceContainer> doGetRootNamespaceContainers(final Type requiredType) {
    final Iterable<Map.Entry<String, NameSpaceContainer>> containerEntries;

    // if a scarce type, use the index.
    if(requiredType != Type.DATASET) {
      containerEntries = namespace.find(new LegacyFindByCondition().setCondition(SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName(), requiredType.getNumber())));
    } else {
      containerEntries = namespace.find(new LegacyFindByRange<>(NamespaceInternalKey.getRootLookupStartKey(), false, NamespaceInternalKey.getRootLookupEndKey(), false));
    }

    final List<NameSpaceContainer> containers = Lists.newArrayList();
    for (final Map.Entry<String, NameSpaceContainer> entry : containerEntries) {
      final NameSpaceContainer container = entry.getValue();
      if (container.getType() == requiredType) {
        containers.add(container);
      }
    }

    return containers;
  }

  @Override
  public List<SpaceConfig> getSpaces() {
    return Lists.newArrayList(
      Iterables.transform(doGetRootNamespaceContainers(SPACE), new Function<NameSpaceContainer, SpaceConfig>() {
        @Override
        public SpaceConfig apply(NameSpaceContainer input) {
          return input.getSpace();
        }
      })
    );
  }

  @Override
  public List<HomeConfig> getHomeSpaces() {
    return Lists.newArrayList(
      Iterables.transform(doGetRootNamespaceContainers(HOME), new Function<NameSpaceContainer, HomeConfig>() {
        @Override
        public HomeConfig apply(NameSpaceContainer input) {
          return input.getHome();
        }
      })
    );
  }

  @Override
  public List<SourceConfig> getSources() {
    return Lists.newArrayList(
      Iterables.transform(doGetRootNamespaceContainers(SOURCE), new Function<NameSpaceContainer, SourceConfig>() {
        @Override
        public SourceConfig apply(NameSpaceContainer input) {
          return input.getSource();
        }
      })
    );
  }

  // returns the child containers of the given rootKey as a list
  private List<NameSpaceContainer> listEntity(final NamespaceKey rootKey) throws NamespaceException {
    return FluentIterable.from(iterateEntity(rootKey)).toList();
  }

  // returns the child containers of the given rootKey as an iterable
  private Iterable<NameSpaceContainer> iterateEntity(final NamespaceKey rootKey) throws NamespaceException {
    final NamespaceInternalKey rootInternalKey = new NamespaceInternalKey(rootKey, keyNormalization);
    final Iterable<Map.Entry<String, NameSpaceContainer>> entries = namespace.find(
      new LegacyFindByRange<>(rootInternalKey.getRangeStartKey(), false, rootInternalKey.getRangeEndKey(), false));
    return FluentIterable.from(entries).transform(input -> input.getValue());
  }

  @Override
  public Iterable<NamespaceKey> getAllDatasets(final NamespaceKey root) throws NamespaceException {
    final NameSpaceContainer rootContainer = namespace.get(new NamespaceInternalKey(root, keyNormalization).getKey());
    if (rootContainer == null) {
      return Collections.emptyList();
    }

    if (!isListable(rootContainer.getType())) {
      return Collections.emptyList();
    }

    return () -> new LazyIteratorOverDatasets(root);
  }

  /**
   * Iterator that lazily loads dataset entries in the sub-tree under the given {@link NamespaceUtils#isListable
   * listable} root. This implementation uses depth-first-search algorithm, unlike {@link #traverseEntity} which uses
   * breadth-first-search algorithm. So this avoids queueing up "dataset" containers. Note that "stack" contains only
   * listable containers which have a small memory footprint.
   */
  private final class LazyIteratorOverDatasets implements Iterator<NamespaceKey> {

    private final NamespaceKey root;
    private final Deque<NamespaceKey> stack = Lists.newLinkedList();
    private final LinkedList<NamespaceKey> nextFewKeys = Lists.newLinkedList();

    private LazyIteratorOverDatasets(NamespaceKey root) {
      this.root = root;
      stack.push(root);
    }

    @Override
    public boolean hasNext() {
      if (!nextFewKeys.isEmpty()) {
        return true;
      }

      populateNextFewKeys();
      return !nextFewKeys.isEmpty();
    }

    private void populateNextFewKeys() {
      while (!stack.isEmpty()) {
        final NamespaceKey top = stack.pop();

        final Iterable<NameSpaceContainer> children;
        try {
          children = iterateEntity(top);
        } catch (NamespaceException e) {
          throw new RuntimeException("failed during dataset listing of sub-tree under: " + root);
        }

        for (final NameSpaceContainer child : children) {
          final NamespaceKey childKey = new NamespaceKey(child.getFullPathList());
          if (child.getType() == DATASET) {
            nextFewKeys.add(childKey);
            continue;
          }

          assert isListable(child.getType()) : "child container is not listable type";
          stack.push(childKey);
        }

        if (!nextFewKeys.isEmpty()) {
          // suspend if few keys are loaded
          break;
        }
      }
    }

    @Override
    public NamespaceKey next() {
      final NamespaceKey nextKey = nextFewKeys.pollFirst();
      if (nextKey == null) {
        throw new NoSuchElementException();
      }
      return nextKey;
    }
  }

  @Override
  public int getAllDatasetsCount(NamespaceKey parent) throws NamespaceException {
    int count = 0;
    for (final NameSpaceContainer container : traverseEntity(parent)) {
      if (container.getType() == DATASET) {
        ++count;
      }
    }
    return count;
  }

  @Override
  public BoundedDatasetCount getDatasetCount(NamespaceKey root, long searchTimeLimitMillis, int countLimitToStopSearch)
      throws NamespaceException {
    return getDatasetCountHelper(root, searchTimeLimitMillis, countLimitToStopSearch);
  }

  private BoundedDatasetCount getDatasetCountHelper(final NamespaceKey root, long searchTimeLimitMillis, int remainingCount) throws NamespaceException {
    int count = 0;
    Stopwatch stopwatch = Stopwatch.createStarted();
    final Deque<NamespaceKey> stack = Lists.newLinkedList();
    stack.push(root);
    while (!stack.isEmpty()) {
      final NamespaceKey top = stack.pop();

      int childrenVisited = 0;
      for (final NameSpaceContainer child : iterateEntity(top)) {
        childrenVisited++;
        if (remainingCount <= 0) {
          return new BoundedDatasetCount(count, false, true);
        }

        // Check the remaining time every few entries avoid the frequent System calls which may slow down
        if (childrenVisited % 50 == 0 && stopwatch.elapsed(TimeUnit.MILLISECONDS) >= searchTimeLimitMillis) {
          return new BoundedDatasetCount(count, true, false);
        }

        final NamespaceKey childKey = new NamespaceKey(child.getFullPathList());
        if (child.getType() == DATASET) {
          count++;
          remainingCount--;
          continue;
        }
        stack.push(childKey);
      }

      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) >= searchTimeLimitMillis) {
        return new BoundedDatasetCount(count, true, false);
      }
    }

    return new BoundedDatasetCount(count, false, false);
  }

  @Override
  public List<NameSpaceContainer> list(NamespaceKey root) throws NamespaceException {
    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(root);
    final NameSpaceContainer rootContainer = lastElement(entitiesOnPath);
    if (rootContainer == null) {
      throw new NamespaceNotFoundException(root, "not found");
    }

    if (!isListable(rootContainer.getType())) {
      throw new NamespaceNotFoundException(root, "no listable entity found");
    }
    return doList(root, entitiesOnPath);
  }

  protected List<NameSpaceContainer> doList(NamespaceKey root, List<NameSpaceContainer> entitiesOnPath) throws NamespaceException {


    return listEntity(root);
  }

  @Override
  public List<Integer> getCounts(SearchQuery... queries) throws NamespaceException {
    final List<Integer> counts = namespace.getCounts(queries);
    if (counts.size() != queries.length) {
      throw new NamespaceInvalidStateException(format("Expected to get %d counts, received %d", queries.length, counts.size()));
    }
    return counts;
  }

  // DELETE

  /**
   * Helper method which travels the tree rooted at the given key and deletes all children in a DFS traversal
   * (except the root). In order to delete an entity, user should have edit permissions on its parent,
   * no need of any permission requirements on the entity that is being deleted (Linux FileSystem permission model)
   *
   * @param key
   * @param container
   * @throws NamespaceException
   */
  private void traverseAndDeleteChildren(final NamespaceInternalKey key, final NameSpaceContainer container)
    throws NamespaceException {
    if (!NamespaceUtils.isListable(container.getType())) {
      return;
    }

    for (NameSpaceContainer child : listEntity(key.getPath())) {
      doTraverseAndDeleteChildren(child);
    }
  }

  protected void doTraverseAndDeleteChildren(final NameSpaceContainer child) throws NamespaceException {
    final NamespaceInternalKey childKey =
      new NamespaceInternalKey(new NamespaceKey(child.getFullPathList()), keyNormalization);
    traverseAndDeleteChildren(childKey, child);

    switch (child.getType()) {
      case FOLDER:
        namespace.delete(childKey.getKey(), child.getFolder().getTag());
        break;
      case DATASET:
        namespace.delete(childKey.getKey(), child.getDataset().getTag());
        break;
      default:
        // Only leaf level or intermediate namespace container types are expected here.
        throw new RuntimeException("Unexpected namespace container type: " + child.getType());
    }
  }

  @VisibleForTesting
  NameSpaceContainer deleteEntity(final NamespaceKey path, final Type type, String version, boolean deleteRoot) throws NamespaceException {
    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(path);
    final NameSpaceContainer container = lastElement(entitiesOnPath);
    if (container == null) {
      throw new NamespaceNotFoundException(path, String.format("Entity %s not found", path));
    }
    return doDeleteEntity(path, type, version, entitiesOnPath, deleteRoot);
  }

  protected NameSpaceContainer doDeleteEntity(final NamespaceKey path, final Type type, String version, List<NameSpaceContainer> entitiesOnPath, boolean deleteRoot) throws NamespaceException {
    final NamespaceInternalKey key = new NamespaceInternalKey(path, keyNormalization);
    final NameSpaceContainer container = lastElement(entitiesOnPath);
    traverseAndDeleteChildren(key, container);
    if(deleteRoot) {
      namespace.delete(key.getKey(), version);
    }
    return container;
  }

  @Override
  public void deleteHome(final NamespaceKey sourcePath, String version) throws NamespaceException {
    deleteEntity(sourcePath, HOME, version, true);
  }

  @Override
  public void deleteSourceChildren(final NamespaceKey sourcePath, String version) throws NamespaceException {
    deleteEntity(sourcePath, SOURCE, version, false);
  }

  @Override
  public void deleteSource(final NamespaceKey sourcePath, String version) throws NamespaceException {
    deleteEntity(sourcePath, SOURCE, version, true);
  }

  @Override
  public void deleteSpace(final NamespaceKey spacePath, String version) throws NamespaceException {
    deleteEntity(spacePath, SPACE, version, true);
  }

  @Override
  public void deleteEntity(NamespaceKey entityPath) throws NamespaceException {
    namespace.delete(new NamespaceInternalKey(entityPath, keyNormalization).getKey());
  }

  @Override
  public void deleteDataset(final NamespaceKey datasetPath, String version) throws NamespaceException {
    NameSpaceContainer container = deleteEntity(datasetPath, DATASET, version, true);
    if (container.getDataset().getType() == PHYSICAL_DATASET_SOURCE_FOLDER) {
      // create a folder so that any existing datasets under the folder are now visible
      addOrUpdateFolder(datasetPath,
        new FolderConfig()
          .setFullPathList(datasetPath.getPathComponents())
          .setName(datasetPath.getName())
      );
    }
  }

  @Override
  public void deleteFolder(final NamespaceKey folderPath, String version) throws NamespaceException {
    deleteEntity(folderPath, FOLDER, version, true);
  }

  @Override
  public DatasetConfig renameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath) throws NamespaceException {
    return doRenameDataset(oldDatasetPath, newDatasetPath);
  }

  protected DatasetConfig doRenameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath) throws NamespaceException {
    final String newDatasetName = newDatasetPath.getName();
    final NamespaceInternalKey oldKey = new NamespaceInternalKey(oldDatasetPath, keyNormalization);

    final NameSpaceContainer container = getEntity(oldDatasetPath, DATASET);
    final DatasetConfig datasetConfig = container.getDataset();

    if (isPhysicalDataset(datasetConfig.getType())) {
      throw UserException.validationError()
        .message("Failed to rename %s to %s. Renames on physical datasets are not allowed.",
          oldDatasetPath, newDatasetPath)
        .build(logger);
    }

    if (datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE && !newDatasetPath.getPathComponents().get(0).startsWith("@")) {
      throw UserException.validationError()
        .message("Failed to rename %s to %s. You cannot move a uploaded file in your home directory to a space.",
          oldDatasetPath, newDatasetPath)
        .build(logger);
    }
    datasetConfig.setName(newDatasetName);
    datasetConfig.setFullPathList(newDatasetPath.getPathComponents());
    datasetConfig.setLastModified(System.currentTimeMillis());
    // in case of upgrade we may have a version here from previous versions, so clear out
    datasetConfig.setVersion(null);
    datasetConfig.setTag(null);

    final NamespaceEntity newValue = NamespaceEntity.toEntity(DATASET, newDatasetPath, datasetConfig, keyNormalization,
        container.getAttributesList());

    namespace.put(newValue.getPathKey().getKey(), newValue.getContainer());
    namespace.delete(oldKey.getKey());

    return datasetConfig;
  }

  // PHYSICAL DATASETS

  private boolean createSourceFolders(NamespaceKey datasetPath) throws NamespaceException {
    List<String> components = datasetPath.getPathComponents();
    if (components.size() > 2) {
      for (int i = 1; i < components.size() - 1; i++) {
        List<String> fullPathList = components.subList(0, i + 1);
        NamespaceKey key = new NamespaceKey(fullPathList);
        final NamespaceInternalKey keyInternal = new NamespaceInternalKey(key, keyNormalization);
        NameSpaceContainer folderContainer = namespace.get(keyInternal.getKey());

        if (folderContainer == null) {
          try {
            addOrUpdateFolder(key, new FolderConfig()
              .setName(components.get(i))
              .setFullPathList(fullPathList)
            );
            continue;
          } catch (ConcurrentModificationException ex) {
            folderContainer = namespace.get(keyInternal.getKey());
            if (folderContainer == null) {
              logger.warn("Failure while updating physical dataset " + datasetPath, ex);
              return false; // TODO: DX-4490
            }
          }
        }

        // make sure the entity exists at this location is:
        // a folder or
        // a physical dataset created from folder.
        switch (folderContainer.getType()) {
          case FOLDER:
            continue;
          case DATASET:
            if (folderContainer.getDataset().getType() == PHYSICAL_DATASET_SOURCE_FOLDER) {
              continue;
            }
            // Fall-through
          default:
            return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean tryCreatePhysicalDataset(NamespaceKey datasetPath, DatasetConfig datasetConfig, NamespaceAttribute... attributes) throws NamespaceException {
    if (createSourceFolders(datasetPath)) {
      datasetConfig.setSchemaVersion(DatasetHelper.CURRENT_VERSION);
      final NamespaceInternalKey searchKey = new NamespaceInternalKey(datasetPath, keyNormalization);
      NameSpaceContainer existingContainer = namespace.get(searchKey.getKey());
      return doTryCreatePhysicalDataset(datasetPath, datasetConfig, searchKey, existingContainer, attributes);
    }
    return false;
  }

  protected boolean doTryCreatePhysicalDataset(NamespaceKey datasetPath, DatasetConfig datasetConfig,
                                               NamespaceInternalKey searchKey, NameSpaceContainer existingContainer, NamespaceAttribute... attributes) throws NamespaceException {
    if (existingContainer != null) {
      switch (existingContainer.getType()) {
        case DATASET: {
          final DatasetConfig currentConfig = existingContainer.getDataset();
          if (currentConfig != null) {
            if (
              (
                  Objects.equals(currentConfig.getPhysicalDataset(), datasetConfig.getPhysicalDataset()) ||
                  Objects.equals(currentConfig.getPhysicalDataset().getFormatSettings(), datasetConfig.getPhysicalDataset().getFormatSettings())) &&
              Objects.equals(DatasetHelper.getSchemaBytes(currentConfig), DatasetHelper.getSchemaBytes(datasetConfig))) {
              return false;
            }
            datasetConfig.setId(currentConfig.getId());
            datasetConfig.setTag(currentConfig.getTag());
          }
        }
        break;
        case FOLDER:
          // delete the folder as it is being converted to a dataset
          namespace.delete(searchKey.getKey(), existingContainer.getFolder().getTag());
          break;

        default:
          return false;
      }
    }

    try {
      addOrUpdateDataset(datasetPath, datasetConfig, attributes);
    } catch (ConcurrentModificationException e) {
      logger.warn("Failure while updating dataset " + datasetPath, e);
      // No one is checking the return value. TODO DX-4490
      return false;
    }

    return true;
  }

  @Override
  public Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> find(LegacyFindByCondition condition) {
    return Iterables.transform(condition == null ? namespace.find() : namespace.find(condition),
        new Function<Map.Entry<String, NameSpaceContainer>, Map.Entry<NamespaceKey, NameSpaceContainer>>() {
          @Override
          public Map.Entry<NamespaceKey, NameSpaceContainer> apply(Map.Entry<String, NameSpaceContainer> input) {
            return new AbstractMap.SimpleEntry<>(
                new NamespaceKey(input.getValue().getFullPathList()), input.getValue());
          }
        });
  }

  private Iterable<PartitionChunkMetadata> partitionChunkValuesAsMetadata(Iterable<Map.Entry<PartitionChunkId, PartitionChunk>> partitionChunks) {
    return FluentIterable
      .from(partitionChunks)
      .transform(item ->
        item.getValue().hasSplitCount()
          ? new PartitionChunkMetadataImpl(item.getValue(), item.getKey(), () -> multiSplitStore.get(item.getKey()))
          : new LegacyPartitionChunkMetadata(item.getValue())
        );
  }

  @Override
  public Iterable<PartitionChunkMetadata> findSplits(LegacyFindByCondition condition) {
    return partitionChunkValuesAsMetadata(partitionChunkStore.find(condition));
  }

  @Override
  public Iterable<PartitionChunkMetadata> findSplits(LegacyFindByRange<PartitionChunkId> range) {
    return partitionChunkValuesAsMetadata(partitionChunkStore.find(range));
  }

  @Override
  public int getPartitionChunkCount(LegacyFindByCondition condition) {
    return partitionChunkStore.getCounts(condition.getCondition()).get(0);
  }

  @Override
  public String dumpSplits() {
    try {
      StringBuilder builder = new StringBuilder();
      builder.append("Partition Chunks: \n");
      for (Map.Entry<PartitionChunkId, PartitionChunk> partitionChunk : partitionChunkStore.find()) {
        builder.append(format("%s: %s\n", partitionChunk.getKey(), partitionChunk.getValue()));
      }
      builder.append("MultiSplits: \n");
      for (Map.Entry<PartitionChunkId, MultiSplit> multiSplit : multiSplitStore.find()) {
        builder.append(format("%s: %s\n", multiSplit.getKey(), multiSplit.getValue()));
      }
      return builder.toString();
    } catch (Exception e) {
      return "Error: " + e;
    }
  }

  // BFS traversal of a folder/space/home.
  // For debugging purposes only
  private Collection<NameSpaceContainer> traverseEntity(final NamespaceKey root) throws NamespaceException {
    final LinkedList<NameSpaceContainer> toBeTraversed = new LinkedList<>(listEntity(root));
    final LinkedList<NameSpaceContainer> visited = new LinkedList<>();
    while (!toBeTraversed.isEmpty()) {
      final NameSpaceContainer container = toBeTraversed.removeFirst();
      if (NamespaceUtils.isListable(container.getType())) {
        toBeTraversed.addAll(listEntity(new NamespaceKey(container.getFullPathList())));
      }
      visited.add(container);
    }
    return visited;
  }

  // For debugging purposes only
  @Override
  public String dump() {
    try {
      final Iterable<Map.Entry<String, NameSpaceContainer>> containers = namespace.find(new LegacyFindByRange<>(
        NamespaceInternalKey.getRootLookupStartKey(), false, NamespaceInternalKey.getRootLookupEndKey(), false));

      final List<NamespaceInternalKey> sourcesKeys = new ArrayList<>();
      final List<NamespaceInternalKey> spacesKeys = new ArrayList<>();
      final List<NamespaceInternalKey> homeKeys = new ArrayList<>();

      for (Map.Entry<String, NameSpaceContainer> entry : containers) {
        try {
          Type type = entry.getValue().getType();
          switch (type) {
            case HOME:
              homeKeys.add(parseKey(entry.getKey().getBytes(StandardCharsets.UTF_8)));
              break;
            case SOURCE:
              sourcesKeys.add(parseKey(entry.getKey().getBytes(StandardCharsets.UTF_8)));
              break;
            case SPACE:
              spacesKeys.add(parseKey(entry.getKey().getBytes(StandardCharsets.UTF_8)));
              break;
            default:
          }
        } catch (Exception e) {
          // no op
        }
      }

      StringBuilder builder = new StringBuilder();
      builder.append("Sources");
      builder.append("\n");
      for (NamespaceInternalKey key : sourcesKeys) {
        builder.append(getSource(key.getPath()).toString());
        builder.append("\n");
        for (NameSpaceContainer container : traverseEntity(key.getPath())) {
          builder.append(container.toString());
          builder.append("\n");
        }
      }
      builder.append("Spaces");
      builder.append("\n");
      for (NamespaceInternalKey key : spacesKeys) {
        builder.append(getSpace(key.getPath()).toString());
        builder.append("\n");
        for (NameSpaceContainer container : traverseEntity(key.getPath())) {
          builder.append(container.toString());
          builder.append("\n");
        }
      }
      builder.append("Homes");
      builder.append("\n");
      for (NamespaceInternalKey key : homeKeys) {
        builder.append(getHome(key.getPath()).toString());
        builder.append("\n");
        for (NameSpaceContainer container : traverseEntity(key.getPath())) {
          builder.append(container.toString());
          builder.append("\n");
        }
      }
      return builder.toString();
    } catch (Exception e) {
      return "Error: " + e;
    }
  }

  /**
   * Helper method that returns the list of the namespace entities on the given path. Except the last entity on the path
   * all other entities should return a non-null value.
   *
   * @param entityPath
   * @return
   */
  protected List<NameSpaceContainer> getEntitiesOnPath(NamespaceKey entityPath) throws NamespaceException {

    final List<String> keys = Lists.newArrayListWithExpectedSize(entityPath.getPathComponents().size());

    NamespaceKey currentPath = entityPath;
    for (int i = 0; i < entityPath.getPathComponents().size(); i++) {
      keys.add(new NamespaceInternalKey(currentPath, keyNormalization).getKey());

      if (currentPath.hasParent()) {
        currentPath = currentPath.getParent();
      }
    }

    // reverse the keys so that the order of keys is from root to leaf level entity.
    Collections.reverse(keys);

    final List<NameSpaceContainer> entitiesOnPath = namespace.get(keys);
    for (int i = 0; i < entitiesOnPath.size() - 1; i++) {
      if (entitiesOnPath.get(i) == null) {
        throw new NamespaceNotFoundException(entityPath, "one or more elements on the path are not found in namespace");
      }
    }

    return entitiesOnPath;
  }

  /**
   * find a container using index
   * @param key
   * @param value
   * @return
   */
  private NameSpaceContainer getByIndex(final IndexKey key, final String value) {
    final SearchQuery query = SearchQueryUtils.newTermQuery(key, value);

    final LegacyFindByCondition condition = new LegacyFindByCondition()
        .setOffset(0)
        .setLimit(1)
        .setCondition(query);

    final Iterable<Entry<String, NameSpaceContainer>> result = namespace.find(condition);
    final Iterator<Entry<String, NameSpaceContainer>> it = result.iterator();
    return it.hasNext()?it.next().getValue():null;
  }

  public static String getKey(NamespaceKey key) {
    return new NamespaceInternalKey(key).getKey();
  }
}

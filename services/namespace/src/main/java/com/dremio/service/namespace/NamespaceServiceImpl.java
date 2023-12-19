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

import static com.dremio.service.namespace.NamespaceUtils.getIdOrNull;
import static com.dremio.service.namespace.NamespaceUtils.isListable;
import static com.dremio.service.namespace.NamespaceUtils.isPhysicalDataset;
import static com.dremio.service.namespace.NamespaceUtils.lastElement;
import static com.dremio.service.namespace.NamespaceUtils.setId;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.DATASET;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.FOLDER;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.FUNCTION;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.HOME;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SOURCE;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SPACE;
import static java.lang.String.format;

import java.io.IOException;
import java.io.OutputStream;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.xerial.snappy.SnappyOutputStream;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.DocumentConverter;
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
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvents;
import com.dremio.service.namespace.catalogstatusevents.events.DatasetDeletionCatalogStatusEvent;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.MultiSplit;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.function.proto.FunctionConfig;
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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Namespace management.
 */
public class NamespaceServiceImpl implements NamespaceService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NamespaceServiceImpl.class);
  public static final String DAC_NAMESPACE = "dac-namespace";
  // NOTE: the name of the partition chunks store needs to stay "metadata-dataset-splits" for backwards compatibility.
  public static final String PARTITION_CHUNKS = "metadata-dataset-splits";
  public static final String MULTI_SPLITS = "metadata-multi-splits";
  private static final int LOG_BATCH = 99;
  public static final int LATEST_VERSION = 1;
  public static final int MAX_ENTITIES_PER_QUERY = 1000;
  private static final int MAX_EXCEPTIONS_ALLOWED = 100;
  private static final int NUM_EXAMINED_SPLITS_BEFORE_LOGGING = 1_000_000;
  private static final int MAX_DELETE_SPLIT_RETRIES = 1;

  private final LegacyIndexedStore<String, NameSpaceContainer> namespace;
  private final LegacyIndexedStore<PartitionChunkId, PartitionChunk> partitionChunkStore;
  private final LegacyKVStore<PartitionChunkId, MultiSplit> multiSplitStore;
  private final CatalogStatusEvents catalogStatusEvents;

  /**
   * Factory for {@code NamespaceServiceImpl}
   */
  public static final class Factory implements NamespaceService.Factory {
    private final LegacyKVStoreProvider kvStoreProvider;
    private final CatalogStatusEvents catalogStatusEvents;

    @Inject
    public Factory(final LegacyKVStoreProvider kvStoreProvider,
                   final CatalogStatusEvents catalogStatusEvents) {
      this.kvStoreProvider = kvStoreProvider;
      this.catalogStatusEvents = catalogStatusEvents;
    }

    @Override
    public NamespaceService get(String userName) {
      Preconditions.checkNotNull(userName, "requires userName"); // per method contract
      return new NamespaceServiceImpl(kvStoreProvider, catalogStatusEvents);
    }

    @Override
    public NamespaceService get(NamespaceIdentity identity) {
      Preconditions.checkNotNull(identity, "requires identity"); // per method contract
      return new NamespaceServiceImpl(kvStoreProvider, catalogStatusEvents);
    }
  }

  @Inject
  public NamespaceServiceImpl(
    final LegacyKVStoreProvider kvStoreProvider,
    final CatalogStatusEvents catalogStatusEvents) {
    this.namespace = createStore(kvStoreProvider);
    this.partitionChunkStore = kvStoreProvider.getStore(PartitionChunkCreator.class);
    this.multiSplitStore = kvStoreProvider.getStore(MultiSplitStoreCreator.class);
    this.catalogStatusEvents = catalogStatusEvents;
  }

  LegacyIndexedStore<String, NameSpaceContainer> getNamespaceStore() {
    return namespace;
  }

  protected LegacyIndexedStore<String, NameSpaceContainer> createStore(final LegacyKVStoreProvider kvStoreProvider) {
    return kvStoreProvider.getStore(NamespaceStoreCreator.class);
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
        .buildIndexed(getConverter());
    }

    protected DocumentConverter<String, NameSpaceContainer> getConverter() {
      return new NamespaceConverter();
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

  /**
   * Delete split with retry
   */
  private void deleteSplitWithRetry(final LegacyKVStore store, final PartitionChunkId key, int numberOfRetries, ExpiredSplitsTracker expiredSplitsTracker) throws RuntimeException {
    while (true) {
      try {
        store.delete(key);
        return;
      } catch (final RuntimeException exception) {
        expiredSplitsTracker.trackException(exception);
        if (--numberOfRetries < 0) {
          throw exception;
        }
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ignore) {
        }
      }
    }
  }

  private void processSplit(final ArrayList<LegacyKVStore> kvStores, PartitionChunkId id, List<Range<PartitionChunkId>> splitsToRetain, ExpiredSplitsTracker expiredSplitsTracker) {
    final int item = Collections.binarySearch(splitsToRetain, Range.singleton(id), PARTITION_CHUNK_RANGE_COMPARATOR);

    // we should never find a match since we're searching for a split key and that dataset
    // split range endpoints are excluded/not valid split keys
    Preconditions.checkState(item < 0);

    final int insertionPoint = (-item) - 1;
    final int consideredRange = insertionPoint - 1; // since a normal match would come directly after the start range, we need to check the range directly above the insertion point.

    boolean deleteSplit = (consideredRange < 0 || !splitsToRetain.get(consideredRange).contains(id));
    try {
      if (deleteSplit) {
        // delete from the kvstore in order
        for (int i = 0; i < kvStores.size(); i++) {
          deleteSplitWithRetry(kvStores.get(i), id, MAX_DELETE_SPLIT_RETRIES, expiredSplitsTracker);
        }
      }
    } finally {
      expiredSplitsTracker.trackPartitionChunk(id, deleteSplit);
    }
  }

  Optional<MetadataPolicy> getMetadataPolicyForDataset(
    final DatasetConfig dataset,
    final Map<String, SourceConfig> sourceConfigs,
    Set<String> unknownSources,
    String unknownSourceMessage) {
    final String sourceName = dataset.getFullPathList().get(0).toLowerCase(Locale.ROOT);
    SourceConfig source = sourceConfigs.get(sourceName);
    if (source == null) {
      if (unknownSources.add(sourceName)) {
        logger.info("Source {} not found for dataset {}. {}", sourceName, PathUtils.constructFullPath(dataset.getFullPathList()), unknownSourceMessage);
      }

      return Optional.empty();
    }

    return Optional.of(source.getMetadataPolicy());
  }

  @Override
  @WithSpan
  public int deleteSplitOrphans(PartitionChunkId.SplitOrphansRetentionPolicy policy, boolean datasetMetadataConsistencyValidate) {
    final Map<String, SourceConfig> sourceConfigs = new HashMap<>();
    final List<Range<PartitionChunkId>> ranges = new ArrayList<>();

    // Iterate over all entries in the namespace to collect source
    // and datasets with splits to create a map of the valid split ranges.
    final Set<String> missingSources = new HashSet<>();
    final Set<String> maybeMissingSources = new HashSet<>();
    // For a given (PDS, Source), the below logic is optimised for when the source is returned first by the iterator
    // When the PDS is returned first by the iterator, such PDSes are tracked here for later processing
    final List<DatasetConfig> datasetsWithUnknownSources = new ArrayList<>();
    for (Map.Entry<String, NameSpaceContainer> entry : namespace.find()) {
      NameSpaceContainer container = entry.getValue();
      switch (container.getType()) {
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
              Optional<MetadataPolicy> datasetPolicy = getMetadataPolicyForDataset(dataset, sourceConfigs, maybeMissingSources, "Will check again");
              if (!datasetPolicy.isPresent()) {
                // remember the dataset for later processing
                datasetsWithUnknownSources.add(dataset);
                continue;
              }
              metadataPolicy = datasetPolicy.get();
              break;

            case VIRTUAL_DATASET:
              // Virtual datasets don't have splits
              continue;

            default:
              logger.error("Unknown dataset type {}. Cannot check for orphan splits", dataset.getType());
              return 0;
          }

          Range<PartitionChunkId> versionRange = policy.apply(metadataPolicy, dataset);
          logger.debug("Split range to retain for dataset {} is {}", PathUtils.constructFullPath(dataset.getFullPathList()), versionRange);
          // min version is based on when dataset definition would expire
          // but it has to be at least positive
          ranges.add(versionRange);

          if (datasetMetadataConsistencyValidate) {
            logger.info("Deleting splitOrphans dataset {} with valid version {}.", dataset.getFullPathList(), versionRange);
          }

          continue;

        default:
      }
    }

    maybeMissingSources.clear();

    // Process the datasets for which sources were unknown in the first iteration and finalise the range for these datasets
    for (DatasetConfig dataset : datasetsWithUnknownSources) {
      Optional<MetadataPolicy> datasetPolicy = getMetadataPolicyForDataset(dataset, sourceConfigs, missingSources, "Considering it as orphaned");
      if (!datasetPolicy.isPresent()) {
        continue;
      }

      final MetadataPolicy metadataPolicy = datasetPolicy.get();
      Range<PartitionChunkId> versionRange = policy.apply(metadataPolicy, dataset);
      logger.debug("Split range to retain for dataset {} is {}", PathUtils.constructFullPath(dataset.getFullPathList()), versionRange);
      // min version is based on when dataset definition would expire
      // but it has to be at least positive
      ranges.add(versionRange);

      if (datasetMetadataConsistencyValidate) {
        logger.info("Deleting splitOrphans dataset {} with valid version {}.", dataset.getFullPathList(), versionRange);
      }
    }

    datasetsWithUnknownSources.clear();
    missingSources.clear();
    sourceConfigs.clear();

    // Ranges need to be sorted for binary search to be working
    Collections.sort(ranges, PARTITION_CHUNK_RANGE_COMPARATOR);

    // Some explanations:
    // ranges is set up to contain the current (exclusive) range of partition chunks for each dataset.
    // The function then iterates over all partition chunks present in the partition chunks store and verify
    // that the partition chunk belongs to one of the dataset partition chunk ranges. If not, the item is dropped.
    // The binary search provides the index in ranges where a partition chunk would be inserted if not
    // already present in the list (this is the insertion point, see Collections.binarySort
    // javadoc), which should be just after the corresponding dataset range as ranges items
    // are sorted based on their lower endpoint.

    // Any key that is expired in multiSplitStore is also expired in partitionChunkStore. This fact is used to
    // identify orphaned splits from multiSplitStore and delete from both stores at the same time
    //
    // multiSplitStore has less load since this store is looked up only for specific splits after searching through
    // partitionChunkStore to identify the splits to read
    try (ExpiredSplitsTracker expiredSplitsTracker = new ExpiredMultiSplitTracker(datasetMetadataConsistencyValidate)) {
      final ArrayList<LegacyKVStore> kvStores = new ArrayList<LegacyKVStore>(2);
      // delete from partitionChunkStore before deleting from multiSplitStore
      kvStores.add(partitionChunkStore);
      kvStores.add(multiSplitStore);
      for (Map.Entry<PartitionChunkId, MultiSplit> e : multiSplitStore.find()) {
        PartitionChunkId id = e.getKey();

        try {
          processSplit(kvStores, id, ranges, expiredSplitsTracker);
        } catch (final RuntimeException exception) {
          if (expiredSplitsTracker.getNumExceptionsIgnored() > MAX_EXCEPTIONS_ALLOWED) {
            logger.warn("Maximum number of exceptions occurred in deleting multi splits. The last exception is: ", exception);
            break;
          }
        }

        if ((expiredSplitsTracker.getSplitsExamined() % NUM_EXAMINED_SPLITS_BEFORE_LOGGING) == 0) {
          // log after examining every million splits
          logger.info("Examined {} multi splits, deleted {}",
            expiredSplitsTracker.getSplitsExamined(), expiredSplitsTracker.getDeletedSplits());
        }
      }

      if (expiredSplitsTracker.getDeletedSplits() > 0) {
        logger.info("Examined {} multi splits in total, deleted {} multi splits",
          expiredSplitsTracker.getSplitsExamined(), expiredSplitsTracker.getDeletedSplits());
      }
      if (expiredSplitsTracker.getNumExceptionsIgnored() > 0) {
        logger.warn("{} exceptions occurred and ignored in deleting multi splits.",
          expiredSplitsTracker.getNumExceptionsIgnored());
      }
    }

    int numPartitionChunksDeleted;
    try (ExpiredSplitsTracker expiredSplitsTracker = new ExpiredSplitsTracker("partition chunks", datasetMetadataConsistencyValidate)) {
      final ArrayList<LegacyKVStore> kvStores = new ArrayList<LegacyKVStore>(1);
      kvStores.add(partitionChunkStore);
      for (Map.Entry<PartitionChunkId, PartitionChunk> e : partitionChunkStore.find()) {
        PartitionChunkId id = e.getKey();

        try {
          processSplit(kvStores, id, ranges, expiredSplitsTracker);
        } catch (final RuntimeException exception) {
          if (expiredSplitsTracker.getNumExceptionsIgnored() > MAX_EXCEPTIONS_ALLOWED) {
            logger.warn("Maximum number of exceptions occurred in deleting partition chunks. The last exception is: ", exception);
            break;
          }
        }

        if ((expiredSplitsTracker.getSplitsExamined() % NUM_EXAMINED_SPLITS_BEFORE_LOGGING) == 0) {
          // log after examining every million splits
          logger.info("Examined {} partition chunks, deleted {}",
            expiredSplitsTracker.getSplitsExamined(), expiredSplitsTracker.getDeletedSplits());
        }
      }

      numPartitionChunksDeleted = expiredSplitsTracker.getDeletedSplits();
      if (expiredSplitsTracker.getDeletedSplits() > 0) {
        logger.info("Examined {} partition chunks in total, deleted {} partition chunks",
          expiredSplitsTracker.getSplitsExamined(), expiredSplitsTracker.getDeletedSplits());
      }
      if (expiredSplitsTracker.getNumExceptionsIgnored() > 0) {
        logger.warn("{} exceptions occurred and ignored in deleting partition chunks.",
          expiredSplitsTracker.getNumExceptionsIgnored());
      }
    }

    return numPartitionChunksDeleted;
  }

  protected LegacyIndexedStore<String, NameSpaceContainer> getStore() {
    return namespace;
  }

  /**
   * Helper method which creates a new entity or update the existing entity with given entity
   *
   * @param entity - The Namespace Entity
   * @throws NamespaceException
   */
  private void createOrUpdateEntity(final NamespaceEntity entity, NamespaceAttribute... attributes) throws NamespaceException {
    final NamespaceKey entityPath = entity.getPathKey().getPath();

    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(entityPath);
    doCreateOrUpdateEntity(entity, entitiesOnPath, attributes);
  }

  protected void doCreateOrUpdateEntity(final NamespaceEntity entity, List<NameSpaceContainer> entitiesOnPath, NamespaceAttribute... attributes) throws NamespaceException {
    validateEntity(entity, entitiesOnPath);

    // update to latest version
    entity.getContainer().setVersion(LATEST_VERSION);

    namespace.put(entity.getPathKey().getKey(), entity.getContainer());
  }

  private void validateEntity(final NamespaceEntity entity, List<NameSpaceContainer> entitiesOnPath) throws InvalidNamespaceNameException {
    final NameSpaceContainer existingEntity = lastElement(entitiesOnPath);
    final boolean isNewTmpRootEntity = entitiesOnPath.size() == 1
      && existingEntity == null
      && entity.getPathKey().getPath().getRoot().equalsIgnoreCase("tmp");
    if (isNewTmpRootEntity) {
      // Tmp is used as a hardcoded name for new untitled queries (tmp.UNTITLED) so it cannot be allowed for
      // user-created top-level entities. Allow updating it in case they already have one from previous to when this
      // check was added.
      throw new InvalidNamespaceNameException(entity.getPathKey().getPath().getRoot());
    }
    ensureIdExistsTypeMatches(entity, existingEntity);
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
  protected boolean ensureIdExistsTypeMatches(NamespaceEntity newOrUpdatedEntity, NameSpaceContainer existingContainer) {
    final String idInContainer = getIdOrNull(newOrUpdatedEntity.getContainer());

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
      namespace.delete((new NamespaceInternalKey(new NamespaceKey(existingContainer.getFullPathList()))).getKey(),
        existingContainer.getFolder().getTag());
      return false;
    }

    if (
      // make sure the type of the existing entity and new entity are the same
      newOrUpdatedEntity.getContainer().getType() != existingContainer.getType() ||
        // If the id in new container is null, then it must be that the user is trying to create another entry of same type at the same path.
        idInContainer == null) {
      List<String> existingPathList = existingContainer.getFullPathList();
      throw UserException.concurrentModificationError()
        .message("The current location already contains a %s named \"%s\". Please use a unique name for the new %s.",
          existingContainer.getType().toString().toLowerCase(), existingPathList.get(existingPathList.size() - 1),
          newOrUpdatedEntity.getContainer().getType().toString().toLowerCase())
        .build(logger);
    }

    NameSpaceContainerVersionExtractor extractor = new NameSpaceContainerVersionExtractor();
    // Note, this duplicates the version check operation that is done inside the kvstore.
    final String newVersion = extractor.getTag(newOrUpdatedEntity.getContainer());
    final String oldVersion = extractor.getTag(existingContainer);
    if (!Objects.equals(newVersion, oldVersion)) {
      final String expectedAction = newVersion == null ? "create" : "update version " + newVersion;
      final String previousValueDesc = oldVersion == null ? "no previous version" : "previous version " + oldVersion;
      throw new ConcurrentModificationException(format("tried to %s, found %s", expectedAction, previousValueDesc));
    }

    // make sure the id remains the same
    final String idInExistingContainer = getIdOrNull(existingContainer);
    // TODO: Could throw a null pointer exception if idInExistingContainer == null.
    if (!idInExistingContainer.equals(idInContainer)) {
      throw UserException.invalidMetadataError().message("There already exists an entity of type [%s] at given path [%s] with Id %s. Unable to replace with Id %s",
        existingContainer.getType(), newOrUpdatedEntity.getPathKey().getPath(), idInExistingContainer, idInContainer).buildSilently();
    }

    return true;
  }

  @Override
  public void addOrUpdateSource(NamespaceKey sourcePath, SourceConfig sourceConfig, NamespaceAttribute... attributes) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(SOURCE, sourcePath, sourceConfig, new ArrayList<>()), attributes);
  }

  @Override
  public void addOrUpdateSpace(NamespaceKey spacePath, SpaceConfig spaceConfig, NamespaceAttribute... attributes) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(SPACE, spacePath, spaceConfig, new ArrayList<>()), attributes);
  }

  @Override
  public void addOrUpdateFunction(NamespaceKey functionPath, FunctionConfig functionConfig, NamespaceAttribute... attributes) throws NamespaceException {
    if (functionConfig.getCreatedAt() == null) {
      functionConfig.setCreatedAt(System.currentTimeMillis());
    }
    functionConfig.setLastModified(System.currentTimeMillis());
    createOrUpdateEntity(NamespaceEntity.toEntity(FUNCTION, functionPath, functionConfig, new ArrayList<>()), attributes);
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
        if (dataset.getPhysicalDataset() == null) {
          dataset.setPhysicalDataset(new PhysicalDataset());
        }
      }
      break;
      case PHYSICAL_DATASET_HOME_FILE:
      case PHYSICAL_DATASET_HOME_FOLDER: {
        if (dataset.getPhysicalDataset() == null) {
          dataset.setPhysicalDataset(new PhysicalDataset());
        }
      }
      break;
      default:
        break;
    }

    createOrUpdateEntity(NamespaceEntity.toEntity(DATASET, datasetPath, dataset, new ArrayList<>()), attributes);
  }

  @Override
  public boolean hasChildren(NamespaceKey key) {
    final Iterable<NameSpaceContainer> children;
    try {
      children = iterateEntity(key);
    } catch (NamespaceException e) {
      throw new RuntimeException("failed during dataset listing of sub-tree under: " + key);
    }
    return FluentIterable.from(children).size() > 0;
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
    private final boolean datasetMetadataConsistencyValidate;

    DatasetMetadataSaverImpl(NamespaceKey datasetPath, EntityId datasetId, long nextDatasetVersion, SplitCompression splitCompression, long maxSinglePartitionChunks, boolean datasetMetadataConsistencyValidate) {
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
      this.datasetMetadataConsistencyValidate = datasetMetadataConsistencyValidate;
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

    @Override
    public void saveDatasetSplit(DatasetSplit split) {
      if (isClosed) {
        throw new IllegalStateException("Attempting to save a dataset split after the saver was closed");
      }
      accumulatedRecordCount += split.getRecordCount();
      accumulatedSizeInBytes += split.getSizeInBytes();
      accumulatedSplits.add(split);
    }

    @Override
    public void savePartitionChunk(com.dremio.connector.metadata.PartitionChunk partitionChunk) throws IOException {
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
     *
     * @return
     */
    private MultiSplit createMultiSplitFromAccumulated(String splitKey) throws IOException {
      ByteString.Output output = ByteString.newOutput();
      OutputStream wrappedOutput = wrapIfNeeded(output);
      for (DatasetSplit split : accumulatedSplits) {
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
          if (datasetMetadataConsistencyValidate) {
            logger.info("Dataset saver hit CME with datasetId {} fullPath {} opportunisticSave {}.", datasetConfig.getId(), datasetConfig.getFullPathList(), opportunisticSave);
          }
          if (opportunisticSave) {
            throw cme;
          }
          if (datasetMetadataConsistencyValidate) {
            logger.info("Dataset {} saver hit CME with opportunisticSave false, nextDatasetVersion {}.", datasetConfig.getId(), nextDatasetVersion);
          }
          // Get dataset config again
          final DatasetConfig existingDatasetConfig = NamespaceServiceImpl.this.getDataset(datasetPath);
          if (existingDatasetConfig.getReadDefinition() != null &&
            existingDatasetConfig.getReadDefinition().getSplitVersion() != null &&
            // Only delete splits if strictly newer. If splitVersions are equals, we
            // could end up delete the splits of the existing dataset (see DX-12232)
            existingDatasetConfig.getReadDefinition().getSplitVersion() > nextDatasetVersion) {
            if (datasetMetadataConsistencyValidate) {
              logger.info("Dataset saver hit CME with datasetId {} with existing splitVersion {}, numSplits {}.", datasetConfig.getId(), existingDatasetConfig.getReadDefinition().getSplitVersion(), existingDatasetConfig.getTotalNumSplits());
            }
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
      if (datasetMetadataConsistencyValidate) {
        LegacyFindByRange<PartitionChunkId> filter = PartitionChunkId.getSplitsRange(datasetConfig.getId(), datasetConfig.getReadDefinition().getSplitVersion());
        for (PartitionChunkMetadata partitionChunkMetadata : findSplits(filter)) {
          partitionChunkMetadata.checkPartitionChunkMetadataConsistency();
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
  public DatasetMetadataSaver newDatasetMetadataSaver(NamespaceKey datasetPath, EntityId datasetId, SplitCompression splitCompression, long maxSinglePartitionChunks, boolean datasetMetadataConsistencyValidate) {
    return new DatasetMetadataSaverImpl(datasetPath, datasetId, System.currentTimeMillis(), splitCompression, maxSinglePartitionChunks, datasetMetadataConsistencyValidate);
  }

  /**
   * Return true if new splits are not same as old splits
   *
   * @param dataset   dataset config
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
    for (PartitionChunk newSplit : newSplits) {
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

  @VisibleForTesting
  void directInsertLegacySplit(DatasetConfig dataset, PartitionChunk split, long splitVersion) {
    final PartitionChunkId splitId = PartitionChunkId.of(dataset, split, splitVersion);
    partitionChunkStore.put(splitId, split);
  }

  @Override
  public void deleteSplits(Iterable<PartitionChunkId> splits) {
    for (PartitionChunkId split : splits) {
      partitionChunkStore.delete(split);
      multiSplitStore.delete(split);
    }
  }

  @Override
  @WithSpan
  public void addOrUpdateFolder(NamespaceKey folderPath, FolderConfig folderConfig, NamespaceAttribute... attributes) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(FOLDER, folderPath, folderConfig, new ArrayList<>()), attributes);
  }

  @Override
  @WithSpan
  public void addOrUpdateHome(NamespaceKey homePath, HomeConfig homeConfig) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(HOME, homePath, homeConfig, new ArrayList<>()));
  }

  @Override
  public List<NameSpaceContainer> getEntities(List<NamespaceKey> lookupKeys) throws NamespaceNotFoundException {
    return doGetEntities(lookupKeys);
  }

  @Override
  public DatasetConfig findDatasetByUUID(String uuid) {
    NameSpaceContainer namespaceContainer = getByIndex(DatasetIndexKeys.DATASET_UUID, uuid);
    return (namespaceContainer != null) ? namespaceContainer.getDataset() : null;
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
  public String getEntityIdByPath(NamespaceKey datasetPath) throws NamespaceNotFoundException {
    final List<NameSpaceContainer> entities = getEntities(Collections.singletonList(datasetPath));
    NameSpaceContainer entity = entities.get(0);

    return entity != null ? NamespaceUtils.getIdOrNull(entity) : null;
  }

  @Override
  public NameSpaceContainer getEntityByPath(NamespaceKey entityPath)
    throws NamespaceException {
    final List<NameSpaceContainer> entities = getEntities(Collections.singletonList(entityPath));

    return entities.get(0);
  }

  protected List<NameSpaceContainer> doGetEntities(List<NamespaceKey> lookupKeys) {
    final List<String> keys = lookupKeys.stream()
      .map(input -> new NamespaceInternalKey(input).getKey()).collect(Collectors.toList());

    return namespace.get(keys);
  }

  // GET

  @Override
  @WithSpan
  public boolean exists(final NamespaceKey key, final Type type) {
    final NameSpaceContainer container = namespace.get(new NamespaceInternalKey(key).getKey());
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
   * @param type
   * @return
   * @throws NamespaceNotFoundException
   */
  private NameSpaceContainer getEntity(final NamespaceKey key, Type type) throws NamespaceNotFoundException {
    final NameSpaceContainer container = getEntity(key);

    if (container == null || container.getType() != type) {
      throw new NamespaceNotFoundException(key, "not found");
    }

    return container;
  }

  NameSpaceContainer getEntity(final NamespaceKey key) throws NamespaceNotFoundException {
    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(key);
    final NameSpaceContainer container = lastElement(entitiesOnPath);

    if (container == null) {
      throw new NamespaceNotFoundException(key, "not found");
    }

    return doGetEntity(entitiesOnPath);
  }

  protected NameSpaceContainer doGetEntity(List<NameSpaceContainer> entitiesOnPath) {
    return lastElement(entitiesOnPath);
  }

  protected NameSpaceContainer getEntityByIndex(IndexKey key, String index, Type type) throws NamespaceException {
    NameSpaceContainer namespaceContainer = getByIndex(key, index);

    if (namespaceContainer == null || namespaceContainer.getType() != type) {
      throw new NamespaceNotFoundException(new NamespaceKey(key.toString()), "not found");
    }

    return namespaceContainer;
  }

  @Override
  @WithSpan
  public SourceConfig getSource(NamespaceKey sourcePath) throws NamespaceException {
    return getEntity(sourcePath, SOURCE).getSource();
  }

  @Override
  @WithSpan
  public SourceConfig getSourceById(String id) throws NamespaceException {
    return getEntityByIndex(NamespaceIndexKeys.SOURCE_ID, id, SOURCE).getSource();
  }

  @Override
  public SpaceConfig getSpace(NamespaceKey spacePath) throws NamespaceException {
    return getEntity(spacePath, SPACE).getSpace();
  }

  @Override
  public FunctionConfig getFunction(NamespaceKey udfPath) throws NamespaceException {
    return getEntity(udfPath, FUNCTION).getFunction();
  }

  @Override
  public SpaceConfig getSpaceById(String id) throws NamespaceException {
    return getEntityByIndex(NamespaceIndexKeys.SPACE_ID, id, SPACE).getSpace();
  }

  @Override
  @WithSpan
  public NameSpaceContainer getEntityById(String id) throws NamespaceNotFoundException {
    SearchQuery query = SearchQueryUtils.or(
      SearchQueryUtils.newTermQuery(DatasetIndexKeys.DATASET_UUID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.SOURCE_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.SPACE_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.HOME_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.FOLDER_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.UDF_ID, id)
    );

    final LegacyFindByCondition condition = new LegacyFindByCondition()
      .setOffset(0)
      .setLimit(1)
      .setCondition(query);

    final Iterable<Entry<String, NameSpaceContainer>> result = namespace.find(condition);
    final Iterator<Entry<String, NameSpaceContainer>> it = result.iterator();
    return it.hasNext() ? it.next().getValue() : null;
  }

  @Override
  public List<NameSpaceContainer> getEntitiesByIds(List<String> ids)
    throws NamespaceNotFoundException {

    if (ids.size() > MAX_ENTITIES_PER_QUERY) {
      throw new IllegalArgumentException(String.format(
        "Maximum %s entities can be fetched per call.",
        MAX_ENTITIES_PER_QUERY));
    }

    SearchQuery query = SearchQueryUtils.or(
      SearchQueryUtils.or(
        ids.stream()
          .map(id -> SearchQueryUtils.newTermQuery(DatasetIndexKeys.DATASET_UUID, id))
          .collect(Collectors.toList())
      ),
      SearchQueryUtils.or(
        ids.stream()
          .map(id -> SearchQueryUtils.newTermQuery(NamespaceIndexKeys.SOURCE_ID, id))
          .collect(Collectors.toList())
      ),
      SearchQueryUtils.or(
        ids.stream()
          .map(id -> SearchQueryUtils.newTermQuery(NamespaceIndexKeys.SPACE_ID, id))
          .collect(Collectors.toList())
      ),
      SearchQueryUtils.or(
        ids.stream()
          .map(id -> SearchQueryUtils.newTermQuery(NamespaceIndexKeys.HOME_ID, id))
          .collect(Collectors.toList())
      ),
      SearchQueryUtils.or(
        ids.stream()
          .map(id -> SearchQueryUtils.newTermQuery(NamespaceIndexKeys.FOLDER_ID, id))
          .collect(Collectors.toList())
      ),
      SearchQueryUtils.or(
        ids.stream()
          .map(id -> SearchQueryUtils.newTermQuery(NamespaceIndexKeys.UDF_ID, id))
          .collect(Collectors.toList())
      ));

    final LegacyFindByCondition condition = new LegacyFindByCondition()
      .setCondition(query);

    final Iterable<Entry<String, NameSpaceContainer>> result = namespace.find(condition);
    final Iterator<Entry<String, NameSpaceContainer>> it = result.iterator();
    List<NameSpaceContainer> entities = new ArrayList<>();
    while (it.hasNext()) {
      entities.add(it.next().getValue());
    }
    return entities;
  }

  @Override
  @WithSpan
  public DatasetConfig getDataset(NamespaceKey datasetPath) throws NamespaceException {
    return getEntity(datasetPath, DATASET).getDataset();
  }

  @Override
  public DatasetConfigAndEntitiesOnPath getDatasetAndEntitiesOnPath(NamespaceKey datasetPath) throws NamespaceException {
    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(datasetPath);
    final NameSpaceContainer container = lastElement(entitiesOnPath);

    if (container == null || container.getType() != DATASET) {
      throw new NamespaceNotFoundException(datasetPath, "not found");
    }

    final DatasetConfig dataset = doGetEntity(entitiesOnPath).getDataset();

    return DatasetConfigAndEntitiesOnPath.of(dataset, entitiesOnPath);
  }

  @Override
  @WithSpan
  public FolderConfig getFolder(NamespaceKey folderPath) throws NamespaceException {
    return getEntity(folderPath, FOLDER).getFolder();
  }

  @Override
  @WithSpan
  public HomeConfig getHome(NamespaceKey homePath) throws NamespaceException {
    return getEntity(homePath, HOME).getHome();
  }

  /**
   * Helper method that returns all root level entities of given type.
   *
   * @return
   */
  protected List<NameSpaceContainer> doGetRootNamespaceContainers(final Type requiredType) {
    final List<NameSpaceContainer> containers = Lists.newArrayList();

    final Iterable<Map.Entry<String, NameSpaceContainer>> containerEntries;
    // if a scarce type, use the index.
    if (requiredType != Type.DATASET) {
      containerEntries = namespace.find(new LegacyFindByCondition().setCondition(SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName(), requiredType.getNumber())));
    } else {
      containerEntries = namespace.find(new LegacyFindByRange<>(NamespaceInternalKey.getRootLookupStartKey(), false, NamespaceInternalKey.getRootLookupEndKey(), false));
    }

    for (final Map.Entry<String, NameSpaceContainer> entry : containerEntries) {
      final NameSpaceContainer container = entry.getValue();
      if (container.getType() == requiredType) {
        containers.add(container);
      }
    }

    return containers;
  }

  @Override
  @WithSpan
  public List<SpaceConfig> getSpaces() {
    final List<SpaceConfig> spaces = Lists.newArrayList(
      Iterables.transform(doGetRootNamespaceContainers(SPACE), new Function<NameSpaceContainer, SpaceConfig>() {
        @Override
        public SpaceConfig apply(NameSpaceContainer input) {
          return input.getSpace();
        }
      })
    );
    Span.current().setAttribute("dremio.namespace.getSpaces.numSpaces", spaces.size());
    return spaces;
  }

  @Override
  public List<FunctionConfig> getFunctions() {
    return Lists.newArrayList(
      Iterables.transform(doGetRootNamespaceContainers(FUNCTION), new Function<NameSpaceContainer, FunctionConfig>() {
        @Override
        public FunctionConfig apply(NameSpaceContainer input) {
          return input.getFunction();
        }
      })
    );
  }

  @Override
  @WithSpan
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
  @WithSpan
  public List<SourceConfig> getSources() {
    final List<SourceConfig> sources = Lists.newArrayList(
      Iterables.transform(doGetRootNamespaceContainers(SOURCE), new Function<NameSpaceContainer, SourceConfig>() {
        @Override
        public SourceConfig apply(NameSpaceContainer input) {
          return input.getSource();
        }
      })
    );
    Span.current().setAttribute("dremio.namespace.getSpaces.numSources", sources.size());
    return sources;
  }

  @Override
  public List<DatasetConfig> getDatasets() {
    final Iterable<Map.Entry<String, NameSpaceContainer>> containerEntries;

    containerEntries = namespace.find();

    final List<DatasetConfig> containers = Lists.newArrayList();
    for (final Map.Entry<String, NameSpaceContainer> entry : containerEntries) {
      final NameSpaceContainer container = entry.getValue();
      if (container.getType() == DATASET) {
        containers.add(container.getDataset());
      }
    }

    return containers;
  }

  // returns the child containers of the given rootKey as a list
  private List<NameSpaceContainer> listEntity(final NamespaceKey rootKey) throws NamespaceException {
    return FluentIterable.from(iterateEntity(rootKey)).toList();
  }

  // returns the child containers of the given rootKey as an iterable
  protected Iterable<NameSpaceContainer> iterateEntity(final NamespaceKey rootKey) throws NamespaceException {
    final NamespaceInternalKey rootInternalKey = new NamespaceInternalKey(rootKey);
    final Iterable<Map.Entry<String, NameSpaceContainer>> entries = namespace.find(
      new LegacyFindByRange<>(rootInternalKey.getRangeStartKey(), false, rootInternalKey.getRangeEndKey(), false));
    return FluentIterable.from(entries).transform(input -> input.getValue());
  }

  @Override
  public Iterable<NamespaceKey> getAllDatasets(final NamespaceKey root) throws NamespaceException {
    final NameSpaceContainer rootContainer = namespace.get(new NamespaceInternalKey(root).getKey());
    if (rootContainer == null) {
      return Collections.emptyList();
    }

    if (!isListable(rootContainer.getType())) {
      return Collections.emptyList();
    }

    return () -> new LazyIteratorOverDatasets(root);
  }

  /**
   * Iterator that lazily loads dataset entries in the subtree under the given {@link NamespaceUtils#isListable
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
  public Iterable<NameSpaceContainer> getAllDescendants(final NamespaceKey root) {
    final NameSpaceContainer rootContainer = namespace.get(new NamespaceInternalKey(root).getKey());
    if (rootContainer == null) {
      return Collections.emptyList();
    }

    if (!isListable(rootContainer.getType())) {
      return Collections.emptyList();
    }

    return () -> new LazyIteratorOverDescendants(root);
  }

  /**
   * Iterator that lazily loads dataset entries in the subtree under the given {@link NamespaceUtils#isListable
   * listable} root. This implementation uses depth-first-search algorithm, unlike {@link #traverseEntity} which uses
   * breadth-first-search algorithm. So this avoids queueing up "dataset" containers. Note that "stack" contains only
   * listable containers which have a small memory footprint.
   */
  private final class LazyIteratorOverDescendants implements Iterator<NameSpaceContainer> {
    private final NamespaceKey root;
    private final Deque<NamespaceKey> stack = Lists.newLinkedList();
    private final LinkedList<NameSpaceContainer> nextFewContainers = Lists.newLinkedList();

    private LazyIteratorOverDescendants(NamespaceKey root) {
      this.root = root;
      stack.push(root);
    }

    @Override
    public boolean hasNext() {
      if (!nextFewContainers.isEmpty()) {
        return true;
      }

      populateNextFewKeys();
      return !nextFewContainers.isEmpty();
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
            nextFewContainers.add(child);
            continue;
          }

          assert isListable(child.getType()) : "child container is not listable type";
          nextFewContainers.add(child);
          stack.push(childKey);
        }

        if (!nextFewContainers.isEmpty()) {
          // suspend if few keys are loaded
          break;
        }
      }
    }

    @Override
    public NameSpaceContainer next() {
      final NameSpaceContainer nextKey = nextFewContainers.pollFirst();
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
  @WithSpan
  public BoundedDatasetCount getDatasetCount(NamespaceKey root, long searchTimeLimitMillis, int countLimitToStopSearch)
    throws NamespaceException {
    return getDatasetCountHelper(root, searchTimeLimitMillis, countLimitToStopSearch, this::iterateEntity);
  }

  public BoundedDatasetCount getDatasetCountHelper(
    final NamespaceKey root, long searchTimeLimitMillis, int remainingCount,
    FunctionWithNamespaceException<NamespaceKey, Iterable<NameSpaceContainer>> iterateEntityFun) throws NamespaceException {
    int count = 0;
    final Stopwatch stopwatch = Stopwatch.createStarted();
    final Deque<NamespaceKey> stack = Lists.newLinkedList();
    stack.push(root);
    while (!stack.isEmpty()) {
      final NamespaceKey top = stack.pop();

      int childrenVisited = 0;
      for (final NameSpaceContainer child : iterateEntityFun.apply(top)) {
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

  @FunctionalInterface
  protected interface FunctionWithNamespaceException<T, R> {
    R apply(T t) throws NamespaceException;
  }

  @Override
  @WithSpan
  public List<NameSpaceContainer> list(NamespaceKey entityPath) throws NamespaceException {
    // TODO: Do we need to get entitiesOnPath?
    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(entityPath);
    final NameSpaceContainer rootContainer = lastElement(entitiesOnPath);
    if (rootContainer == null) {
      throw new NamespaceNotFoundException(entityPath, "not found");
    }

    if (!isListable(rootContainer.getType())) {
      throw new NamespaceNotFoundException(entityPath, "no listable entity found");
    }
    return doList(entityPath);
  }

  protected List<NameSpaceContainer> doList(NamespaceKey root) throws NamespaceException {
    return listEntity(root);
  }

  @Override
  @WithSpan
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
  private void traverseAndDeleteChildren(final NamespaceInternalKey key, final NameSpaceContainer container, DeleteCallback callback) throws NamespaceException {
    if (!NamespaceUtils.isListable(container.getType())) {
      return;
    }

    for (NameSpaceContainer child : listEntity(key.getPath())) {
      doTraverseAndDeleteChildren(child, callback);
    }
  }

  protected void doTraverseAndDeleteChildren(final NameSpaceContainer child, DeleteCallback callback) throws NamespaceException {
    final NamespaceInternalKey childKey =
      new NamespaceInternalKey(new NamespaceKey(child.getFullPathList()));
    traverseAndDeleteChildren(childKey, child, callback);

    switch (child.getType()) {
      case FOLDER:
        namespace.delete(childKey.getKey(), child.getFolder().getTag());
        break;
      case DATASET:
        if (callback != null) {
          callback.onDatasetDelete(child.getDataset());
        }
        namespace.delete(childKey.getKey(), child.getDataset().getTag());
        catalogStatusEvents.publish(new DatasetDeletionCatalogStatusEvent(child.getDataset().toString()));
        break;
      case FUNCTION:
        namespace.delete(childKey.getKey(), child.getFunction().getTag());
        break;
      default:
        // Only leaf level or intermediate namespace container types are expected here.
        throw new RuntimeException("Unexpected namespace container type: " + child.getType());
    }
  }

  @VisibleForTesting
  NameSpaceContainer deleteEntityWithCallback(final NamespaceKey path, String version, boolean deleteRoot,
                                              DeleteCallback callback) throws NamespaceException {
    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(path);
    final NameSpaceContainer container = lastElement(entitiesOnPath);
    if (container == null) {
      throw new NamespaceNotFoundException(path, String.format("Entity %s not found", path));
    }
    return doDeleteEntity(path, version, container, deleteRoot, callback);
  }

  @WithSpan
  protected NameSpaceContainer doDeleteEntity(final NamespaceKey path, String version, NameSpaceContainer container, boolean deleteRoot, DeleteCallback callback) throws NamespaceException {
    final NamespaceInternalKey key = new NamespaceInternalKey(path);
    traverseAndDeleteChildren(key, container, callback);
    if (deleteRoot) {
      namespace.delete(key.getKey(), version);
    }
    return container;
  }

  @Override
  public void deleteHome(final NamespaceKey homePath, String version) throws NamespaceException {
    deleteEntityWithCallback(homePath, version, true, null);
  }

  @Override
  public void deleteSourceChildren(final NamespaceKey sourcePath, String version, DeleteCallback callback) throws NamespaceException {
    deleteEntityWithCallback(sourcePath, version, false, callback);
  }

  @Override
  public void deleteSource(final NamespaceKey sourcePath, String version) throws NamespaceException {
    deleteEntityWithCallback(sourcePath, version, true, null);
  }

  @Override
  public void deleteSourceWithCallBack(final NamespaceKey sourcePath, String version, DeleteCallback callback) throws NamespaceException {
    deleteEntityWithCallback(sourcePath, version, true, callback);
  }

  @Override
  public void deleteSpace(final NamespaceKey spacePath, String version) throws NamespaceException {
    deleteEntityWithCallback(spacePath, version, true, null);
  }

  @Override
  public void deleteFunction(NamespaceKey functionPath) throws NamespaceException {
    deleteEntity(functionPath);
  }


  @Deprecated
  @Override
  public void deleteEntity(NamespaceKey entityPath) throws NamespaceException {
    namespace.delete(new NamespaceInternalKey(entityPath).getKey());
  }

  @Override
  @WithSpan
  public void deleteDataset(final NamespaceKey datasetPath, String version, final NamespaceAttribute... attributes) throws NamespaceException {
    NameSpaceContainer container = deleteEntityWithCallback(datasetPath, version, true, null);
    if (container.getDataset().getType() == PHYSICAL_DATASET_SOURCE_FOLDER) {
      // create a folder so that any existing datasets under the folder are now visible
      addOrUpdateFolder(datasetPath,
        new FolderConfig()
          .setFullPathList(datasetPath.getPathComponents())
          .setName(datasetPath.getName()),
        attributes
      );
    }
    catalogStatusEvents.publish(new DatasetDeletionCatalogStatusEvent(datasetPath.toString()));
  }

  @Override
  @WithSpan
  public void deleteFolder(final NamespaceKey folderPath, String version) throws NamespaceException {
    deleteEntityWithCallback(folderPath, version, true, null);
  }

  @Override
  public DatasetConfig renameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath) throws NamespaceException {
    return doRenameDataset(oldDatasetPath, newDatasetPath);
  }

  protected DatasetConfig doRenameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath) throws NamespaceException {
    final String newDatasetName = newDatasetPath.getName();
    final NamespaceInternalKey oldKey = new NamespaceInternalKey(oldDatasetPath);

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

    final NamespaceEntity newValue = NamespaceEntity.toEntity(DATASET, newDatasetPath, datasetConfig,
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
        final NamespaceInternalKey keyInternal = new NamespaceInternalKey(key);
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
            // fall through
          default:
            return false;
        }
      }
    }
    return true;
  }

  @Override
  @WithSpan
  public boolean tryCreatePhysicalDataset(NamespaceKey datasetPath, DatasetConfig datasetConfig, NamespaceAttribute... attributes) throws NamespaceException {
    if (createSourceFolders(datasetPath)) {
      datasetConfig.setSchemaVersion(DatasetHelper.CURRENT_VERSION);
      final NamespaceInternalKey searchKey = new NamespaceInternalKey(datasetPath);
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
    BatchLookupOptimiser<PartitionChunkId, MultiSplit> optimiser = new BatchLookupOptimiser<>(multiSplitStore::get);
    return FluentIterable
      .from(partitionChunks)
      .transform(item ->
        item.getValue().hasSplitCount()
          ? new PartitionChunkMetadataImpl(item.getValue(), item.getKey(), () -> optimiser.mayLookup(item.getKey()), () -> optimiser.lookup(item.getKey()))
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

  // BFS traversal of a folder/space/home.
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

  /**
   * Helper method that returns the list of the namespace entities on the given path. Except the last entity on the path
   * all other entities should return a non-null value.
   *
   * @param entityPath
   * @return
   */
  protected List<NameSpaceContainer> getEntitiesOnPath(NamespaceKey entityPath) throws NamespaceNotFoundException {
    List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPathWithoutValidation(entityPath);

    for (int i = 0; i < entitiesOnPath.size() - 1; i++) {
      if (entitiesOnPath.get(i) == null) {
        throw new NamespaceNotFoundException(entityPath, "one or more elements on the path are not found in namespace");
      }
    }
    return entitiesOnPath;
  }

  /**
   * Helper method that returns the list of the namespace entities on the given path. Does not validate that values
   * are non-null.
   *
   * @param entityPath
   * @return
   */
  protected List<NameSpaceContainer> getEntitiesOnPathWithoutValidation(NamespaceKey entityPath) throws NamespaceNotFoundException {
    final List<String> keys = Lists.newArrayListWithExpectedSize(entityPath.getPathComponents().size());

    NamespaceKey currentPath = entityPath;
    for (int i = 0; i < entityPath.getPathComponents().size(); i++) {
      keys.add(new NamespaceInternalKey(currentPath).getKey());

      if (currentPath.hasParent()) {
        currentPath = currentPath.getParent();
      }
    }

    // reverse the keys so that the order of keys is from root to leaf level entity.
    Collections.reverse(keys);

    return namespace.get(keys);
  }

  /**
   * find a container using index
   *
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
    return it.hasNext() ? it.next().getValue() : null;
  }

  public static String getKey(NamespaceKey key) {
    return new NamespaceInternalKey(key).getKey();
  }

  /**
   * Return the highest level container in the namespace tree for the given key.
   */
  public Optional<NameSpaceContainer> getRootContainer(NameSpaceContainer container) {
    Optional<NameSpaceContainer> rootContainer = Optional.empty();

    try {
      final NamespaceKey namespaceKey = new NamespaceKey(NamespaceUtils.firstElement(container.getFullPathList()));
      final List<NameSpaceContainer> entitiesOnPath = getEntities(Collections.singletonList(namespaceKey));
      rootContainer = Optional.ofNullable(entitiesOnPath.get(0));
    } catch (NamespaceException ignored) {
      // ignored, empty returned
    }

    return rootContainer;
  }

  // inner class to track expired splits
  // tracks the total number of splits examined, number deleted, the keys themselves if required
  private class ExpiredSplitsTracker implements AutoCloseable {
    // number of examined splits
    private int splitsExamined;
    // number of deleted splits
    private int deletedSplits;
    private boolean verifyMetadataConsistency;
    // buffer for debugging - keeps track of deleted keys
    private final List<String> deletedSplitsForDebug = new ArrayList<>(LOG_BATCH);
    // number of exceptions ignored
    private int numExceptionsIgnored;
    private final String desc;

    ExpiredSplitsTracker(String desc, boolean datasetMetadataConsistencyValidate) {
      this.verifyMetadataConsistency = datasetMetadataConsistencyValidate;
      this.splitsExamined = 0;
      this.deletedSplits = 0;
      this.numExceptionsIgnored = 0;
      this.desc = desc;
    }

    void verifySplitConsistency(PartitionChunkId id) {
    }

    void trackPartitionChunk(PartitionChunkId id, boolean deleteSplit) {
      splitsExamined++;
      if (deleteSplit) {
        deletedSplits++;
        if (verifyMetadataConsistency) {
          if (deletedSplitsForDebug.size() >= LOG_BATCH) {
            logger.info("Deleting {} associated with keys {}.", desc,
              Arrays.toString(deletedSplitsForDebug.toArray()));
            deletedSplitsForDebug.clear();
          }
          deletedSplitsForDebug.add(id.toString());
          verifySplitConsistency(id);
        } else {
          logger.debug("Deleting {} associated with key {} from the store.", desc, id);
        }
      }
    }

    void trackException(final RuntimeException exception) {
      numExceptionsIgnored++;
    }

    public int getSplitsExamined() {
      return splitsExamined;
    }

    public int getDeletedSplits() {
      return deletedSplits;
    }

    public int getNumExceptionsIgnored() {
      return numExceptionsIgnored;
    }

    @Override
    public void close() {
      if (verifyMetadataConsistency) {
        logger.info("Deleting {} associated with keys {}.", desc,
          Arrays.toString(deletedSplitsForDebug.toArray()));
        deletedSplitsForDebug.clear();
      }
    }
  }

  // Tracks expired splits in metadata-multi-splits
  private final class ExpiredMultiSplitTracker extends ExpiredSplitsTracker {
    ExpiredMultiSplitTracker(boolean datasetMetadataConsistencyValidate) {
      super("multi splits", datasetMetadataConsistencyValidate);
    }

    // overrides this method to log a warning if the split to be deleted is present in metadata-dataset-splits
    @Override
    void verifySplitConsistency(PartitionChunkId id) {
      if (partitionChunkStore.contains(id)) {
        logger.warn("MultiSplit being deleted, but PartitionChunk exists for id {}.", id.getSplitId());
      }
    }
  }
}

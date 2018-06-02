/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStore.FindByRange;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.PassThroughSerializer;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

/**
 * Namespace management.
 */
public class NamespaceServiceImpl implements NamespaceService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NamespaceServiceImpl.class);

  public static final String DAC_NAMESPACE = "dac-namespace";
  public static final String DATASET_SPLITS = "metadata-dataset-splits";

  private final IndexedStore<byte[], NameSpaceContainer> namespace;
  private final IndexedStore<DatasetSplitId, DatasetSplit> splitsStore;
  private final boolean keyNormalization;

  /**
   * Factory for {@code NamespaceServiceImpl}
   */
  public static final class Factory implements NamespaceService.Factory {
    private final KVStoreProvider kvStoreProvider;

    @Inject
    public Factory(KVStoreProvider kvStoreProvider) {
      this.kvStoreProvider = kvStoreProvider;
    }

    @Override
    public NamespaceService get(String userName) {
      Preconditions.checkNotNull(userName, "requires userName"); // per method contract
      return new NamespaceServiceImpl(kvStoreProvider);
    }
  }

  @Inject
  public NamespaceServiceImpl(final KVStoreProvider kvStoreProvider) {
    this(kvStoreProvider, true);
  }

  protected NamespaceServiceImpl(final KVStoreProvider kvStoreProvider, boolean keyNormalization) {
    this.namespace = kvStoreProvider.getStore(NamespaceStoreCreator.class);
    this.splitsStore = kvStoreProvider.getStore(DatasetSplitCreator.class);
    this.keyNormalization = keyNormalization;
  }

  /**
   * Creator for name space kvstore.
   */
  public static class NamespaceStoreCreator implements StoreCreationFunction<IndexedStore<byte[], NameSpaceContainer>> {

    @Override
    public IndexedStore<byte[], NameSpaceContainer> build(StoreBuildingFactory factory) {
      return factory.<byte[], NameSpaceContainer>newStore()
        .name(DAC_NAMESPACE)
        .keySerializer(PassThroughSerializer.class)
        .valueSerializer(NameSpaceContainerSerializer.class)
        .versionExtractor(NameSpaceContainerVersionExtractor.class)
        .buildIndexed(NamespaceConverter.class);
    }

  }

  /**
   * KVStore creator for splits table
   */
  public static class DatasetSplitCreator implements StoreCreationFunction<IndexedStore<DatasetSplitId, DatasetSplit>> {

    @Override
    public IndexedStore<DatasetSplitId, DatasetSplit> build(StoreBuildingFactory factory) {
      return factory.<DatasetSplitId, DatasetSplit>newStore()
        .name(DATASET_SPLITS)
        .keySerializer(DatasetSplitIdSerializer.class)
        .valueSerializer(DatasetSplitSerializer.class)
        .buildIndexed(DatasetSplitConverter.class);
    }
  }

  /**
   * Helper class for finding split orphans.
   */
  private static class SplitRange implements Comparable<SplitRange> {

    private final Range<String> range;

    public SplitRange(Range<String> range) {
      super();
      this.range = range;
    }

    @Override
    public int compareTo(SplitRange o) {
      return range.lowerEndpoint().compareTo(o.range.lowerEndpoint());
    }

  }

  @Override
  public int deleteSplitOrphans() {
    final List<SplitRange> ranges = new ArrayList<>();

    int itemsDeleted = 0;
    for(Map.Entry<byte[], NameSpaceContainer> entry : namespace.find()) {
      NameSpaceContainer container = entry.getValue();
      if(container.getType() == Type.DATASET && container.getDataset().getReadDefinition() != null && container.getDataset().getReadDefinition().getSplitVersion() != null) {
        ranges.add(new SplitRange(DatasetSplitId.getSplitStringRange(container.getDataset())));
      }
    }

    for(Map.Entry<DatasetSplitId, DatasetSplit> e : splitsStore.find()) {
      String id = e.getKey().getSplitIdentifier();
      final int item = Collections.binarySearch(ranges, new SplitRange(Range.singleton(id)));

      // we should never find a match since we're searching for a split key.
      Preconditions.checkArgument(item < 0);

      final int insertionPoint = (-item) - 1;
      final int consideredRange = insertionPoint - 1; // since a normal match would come directly after the start range, we need to check the range directly above the insertion point.

      if(consideredRange < 0 || ranges.get(consideredRange).range.contains(id)) {
        splitsStore.delete(e.getKey());
        itemsDeleted++;
      }
    }
    return itemsDeleted;
  }

  /**
   * Helper method which creates a new entity or update the existing entity with given entity
   *
   * @param entity
   * @throws NamespaceException
   */
  private void createOrUpdateEntity(final NamespaceEntity entity) throws NamespaceException {
    final NamespaceKey entityPath = entity.getPathKey().getPath();

    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(entityPath);
    doCreateOrUpdateEntity(entity, entitiesOnPath);
  }

  protected void doCreateOrUpdateEntity(final NamespaceEntity entity, List<NameSpaceContainer> entitiesOnPath) throws NamespaceException {
    final NameSpaceContainer prevContainer = lastElement(entitiesOnPath);
    ensureIdExistsTypeMatches(entity, prevContainer);

    namespace.put(entity.getPathKey().getKey(), entity.getContainer());
  }

  // TODO: Remove this operation and move to kvstore
  private void ensureIdExistsTypeMatches(NamespaceEntity newOrUpdatedEntity, NameSpaceContainer existingContainer) throws NamespaceException{
    final String idInContainer = getId(newOrUpdatedEntity.getContainer());

    if (existingContainer == null) {
      if (idInContainer != null) {
        // If the client already gives an id, take it.
        return;
      }

      // generate a new id
      setId(newOrUpdatedEntity.getContainer(), UUID.randomUUID().toString());
      return;
    }

    // TODO: this is a side effect and shouldn't be done in this method.
    // Folder in source can be marked as a dataset. (folder->dataset)
    // When user removes format settings on such folder, we convert physical dataset back to folder. (dataset->folder)
    // In the case when folder is converted back into dataset, skip type checking and delete folder first and then add dataset.
    if (newOrUpdatedEntity.getContainer().getType() == DATASET &&
      isPhysicalDataset(newOrUpdatedEntity.getContainer().getDataset().getType()) &&
      existingContainer.getType() == FOLDER) {
      namespace.delete((new NamespaceInternalKey(new NamespaceKey(existingContainer.getFullPathList()), keyNormalization)).getKey(),
        existingContainer.getFolder().getVersion());
      return;
    }

    NameSpaceContainerVersionExtractor extractor = new NameSpaceContainerVersionExtractor();
    // let's make sure we aren't dealing with a concurrent update before we try to check other
    // conditions. A concurrent update should throw that exception rather than the user exceptions
    // thrown later in this check. Note, this duplicates the version check operation that is done
    // inside the kvstore.
    final Long newVersion = extractor.getVersion(newOrUpdatedEntity.getContainer());
    final Long oldVersion = extractor.getVersion(existingContainer);
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
      throw UserException.validationError()
        .message("Id for an existing entity cannot be modified")
        .build(logger);
    }
  }

  @Override
  public void addOrUpdateSource(NamespaceKey sourcePath, SourceConfig sourceConfig) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(SOURCE, sourcePath, sourceConfig, keyNormalization));
  }

  @Override
  public void addOrUpdateSpace(NamespaceKey spacePath, SpaceConfig spaceConfig) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(SPACE, spacePath, spaceConfig, keyNormalization));
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset) throws NamespaceException {
    dataset.setSchemaVersion(DatasetHelper.CURRENT_VERSION);

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

    createOrUpdateEntity(NamespaceEntity.toEntity(DATASET, datasetPath, dataset, keyNormalization));
  }

  /**
   * Return true if new splits are not same as old splits
   * @param dataset dataset config
   * @param newSplits new splits to be added
   * @param oldSplits existing old splits
   * @return false if old and new splits are same
   */
  static boolean compareSplits(DatasetConfig dataset,
                               List<DatasetSplit> newSplits,
                               Iterable<Map.Entry<DatasetSplitId, DatasetSplit>> oldSplits) {
    // if both old and new splits are null return false
    if (oldSplits == null && newSplits == null) {
      return false;
    }
    final ImmutableMap.Builder<DatasetSplitId, DatasetSplit> builder = ImmutableMap.builder();
    for (DatasetSplit newSplit: newSplits) {
      final DatasetSplitId splitId = new DatasetSplitId(dataset, newSplit, dataset.getReadDefinition().getSplitVersion());
      // for comparison purpose, use the last known split version
      newSplit.setSplitVersion(dataset.getReadDefinition().getSplitVersion());
      builder.put(splitId, newSplit);
    }
    final ImmutableMap<DatasetSplitId, DatasetSplit> newSplitsMap = builder.build();

    if (oldSplits != null) {
      int oldSplitsCount = 0;
      for (Map.Entry<DatasetSplitId, DatasetSplit> entry : oldSplits) {
        final DatasetSplit newSplit = newSplitsMap.get(entry.getKey());
        if (newSplit == null || !newSplit.equals(entry.getValue())) {
          return true;
        }
        ++oldSplitsCount;
      }
      if (newSplits.size() != oldSplitsCount) {
        return true;
      }
      return false;
    }

    return true;
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset, List<DatasetSplit> splits) throws NamespaceException {
    Preconditions.checkNotNull(dataset.getReadDefinition());

    if (dataset.getReadDefinition() != null && dataset.getReadDefinition().getSplitVersion() != null &&
      !compareSplits(dataset, splits, splitsStore.find(DatasetSplitId.getSplitsRange(dataset)))) {
      addOrUpdateDataset(datasetPath, dataset);
      return;
    }

    final long nextSplitVersion = System.currentTimeMillis();
    final List<DatasetSplitId> splitIds = Lists.newArrayList();
    // only if splits have changed update splits version and retry read definition on concurrent modification.
    for (DatasetSplit split : splits) {
      final DatasetSplitId splitId = new DatasetSplitId(dataset, split, nextSplitVersion);
      split.setSplitVersion(nextSplitVersion);
      splitsStore.put(splitId, split);
      splitIds.add(splitId);
    }
    dataset.getReadDefinition().setSplitVersion(nextSplitVersion);
    while (true) {
      try {
        addOrUpdateDataset(datasetPath, dataset);
        break;
      } catch (ConcurrentModificationException cme) {
        // Get dataset config again
        final DatasetConfig existingDatasetConfig = getDataset(datasetPath);
        if (existingDatasetConfig.getReadDefinition() != null &&
          existingDatasetConfig.getReadDefinition().getSplitVersion() != null &&
          existingDatasetConfig.getReadDefinition().getSplitVersion() >= nextSplitVersion) {
          deleteSplits(splitIds);
          break;
        }
        // try again if read definition is not set or splits are not up-to-date.
        dataset.setVersion(existingDatasetConfig.getVersion());
      }
    }
  }

  @Override
  public void deleteSplits(Iterable<DatasetSplitId> splits) {
    for (DatasetSplitId split: splits) {
      splitsStore.delete(split);
    }
  }

  @Override
  public void addOrUpdateFolder(NamespaceKey folderPath, FolderConfig folderConfig) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(FOLDER, folderPath, folderConfig, keyNormalization));
  }

  @Override
  public void addOrUpdateHome(NamespaceKey homePath, HomeConfig homeConfig) throws NamespaceException {
    createOrUpdateEntity(NamespaceEntity.toEntity(HOME, homePath, homeConfig, keyNormalization));
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

  protected List<NameSpaceContainer> doGetEntities(List<NamespaceKey> lookupKeys) {
    final List<byte[]> keys = FluentIterable.from(lookupKeys).transform(new Function<NamespaceKey, byte[]>() {
      @Override
      public byte[] apply(NamespaceKey input) {
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

  private NameSpaceContainer getEntityByIndex(IndexKey key, String index, Type type) throws NamespaceNotFoundException {
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
  public SourceConfig getSourceById(String id) throws NamespaceNotFoundException {
    return getEntityByIndex(NamespaceIndexKeys.SOURCE_ID, id, SOURCE).getSource();
  }

  @Override
  public SpaceConfig getSpace(NamespaceKey spacePath) throws NamespaceException {
    return getEntity(spacePath, SPACE).getSpace();
  }

  @Override
  public SpaceConfig getSpaceById(String id) throws NamespaceNotFoundException {
    return getEntityByIndex(NamespaceIndexKeys.SPACE_ID, id, SPACE).getSpace();
  }

  @Override
  public NameSpaceContainer getEntityById(String id) throws NamespaceNotFoundException {
    final SearchQuery query = SearchQueryUtils.or(
      SearchQueryUtils.newTermQuery(DatasetIndexKeys.DATASET_UUID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.SOURCE_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.SPACE_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.HOME_ID, id),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.FOLDER_ID, id));

    final IndexedStore.FindByCondition condition = new IndexedStore.FindByCondition()
      .setOffset(0)
      .setLimit(1)
      .setCondition(query);

    final Iterable<Entry<byte[], NameSpaceContainer>> result = namespace.find(condition);
    final Iterator<Entry<byte[], NameSpaceContainer>> it = result.iterator();
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
    final Iterable<Map.Entry<byte[], NameSpaceContainer>> containerEntries;

    // if a scarce type, use the index.
    if(requiredType != Type.DATASET) {
      containerEntries = namespace.find(new FindByCondition().setCondition(SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName(), requiredType.getNumber())));
    } else {
      containerEntries = namespace.find(new FindByRange<>(NamespaceInternalKey.getRootLookupStart(), false, NamespaceInternalKey.getRootLookupEnd(), false));
    }

    final List<NameSpaceContainer> containers = Lists.newArrayList();
    for (final Map.Entry<byte[], NameSpaceContainer> entry : containerEntries) {
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
    final Iterable<Map.Entry<byte[], NameSpaceContainer>> entries = namespace.find(
      new FindByRange<>(rootInternalKey.getRangeStartKey(), false, rootInternalKey.getRangeEndKey(), false));
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
        namespace.delete(childKey.getKey(), child.getFolder().getVersion());
        break;
      case DATASET:
        namespace.delete(childKey.getKey(), child.getDataset().getVersion());
        break;
      default:
        // Only leaf level or intermediate namespace container types are expected here.
        throw new RuntimeException("Unexpected namespace container type: " + child.getType());
    }
  }

  @VisibleForTesting
  NameSpaceContainer deleteEntity(final NamespaceKey path, final Type type, long version, boolean deleteRoot) throws NamespaceException {
    final List<NameSpaceContainer> entitiesOnPath = getEntitiesOnPath(path);
    final NameSpaceContainer container = lastElement(entitiesOnPath);
    if (container == null) {
      throw new NamespaceNotFoundException(path, String.format("Entity %s not found", path));
    }
    return doDeleteEntity(path, type, version, entitiesOnPath, deleteRoot);
  }

  protected NameSpaceContainer doDeleteEntity(final NamespaceKey path, final Type type, long version, List<NameSpaceContainer> entitiesOnPath, boolean deleteRoot) throws NamespaceException {
    final NamespaceInternalKey key = new NamespaceInternalKey(path, keyNormalization);
    final NameSpaceContainer container = lastElement(entitiesOnPath);
    traverseAndDeleteChildren(key, container);
    if(deleteRoot) {
      namespace.delete(key.getKey(), version);
    }
    return container;
  }

  @Override
  public void deleteHome(final NamespaceKey sourcePath, long version) throws NamespaceException {
    deleteEntity(sourcePath, HOME, version, true);
  }

  @Override
  public void deleteSourceChildren(final NamespaceKey sourcePath, long version) throws NamespaceException {
    deleteEntity(sourcePath, SOURCE, version, false);
  }

  @Override
  public void deleteSource(final NamespaceKey sourcePath, long version) throws NamespaceException {
    deleteEntity(sourcePath, SOURCE, version, true);
  }

  @Override
  public void deleteSpace(final NamespaceKey spacePath, long version) throws NamespaceException {
    deleteEntity(spacePath, SPACE, version, true);
  }

  @Override
  public void deleteEntity(NamespaceKey entityPath) throws NamespaceException {
    namespace.delete(new NamespaceInternalKey(entityPath, keyNormalization).getKey());
  }

  @Override
  public void deleteDataset(final NamespaceKey datasetPath, long version) throws NamespaceException {
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
  public void deleteFolder(final NamespaceKey folderPath, long version) throws NamespaceException {
    deleteEntity(folderPath, FOLDER, version, true);
  }

  @Override
  public DatasetConfig renameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath) throws NamespaceException {
    return doRenameDataset(oldDatasetPath, newDatasetPath, getEntitiesOnPath(oldDatasetPath.getParent()), getEntitiesOnPath(newDatasetPath.getParent()));
  }

  protected DatasetConfig doRenameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath,
                                          List<NameSpaceContainer> oldDatasetParentEntitiesOnPath, List<NameSpaceContainer> newDatasetParentEntitiesOnPath) throws NamespaceException {
    final String newDatasetName = newDatasetPath.getName();
    final NamespaceInternalKey oldKey = new NamespaceInternalKey(oldDatasetPath, keyNormalization);

    final DatasetConfig datasetConfig = getEntity(oldDatasetPath, DATASET).getDataset();

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
    NamespaceEntity newValue = NamespaceEntity.toEntity(DATASET, newDatasetPath, datasetConfig, keyNormalization);

    datasetConfig.setVersion(null);

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
  public boolean tryCreatePhysicalDataset(NamespaceKey datasetPath, DatasetConfig datasetConfig) throws NamespaceException {
    if (createSourceFolders(datasetPath)) {
      datasetConfig.setSchemaVersion(DatasetHelper.CURRENT_VERSION);
      final NamespaceInternalKey searchKey = new NamespaceInternalKey(datasetPath, keyNormalization);
      NameSpaceContainer existingContainer = namespace.get(searchKey.getKey());
      return doTryCreatePhysicalDataset(datasetPath, datasetConfig, searchKey, existingContainer);
    }
    return false;
  }

  protected boolean doTryCreatePhysicalDataset(NamespaceKey datasetPath, DatasetConfig datasetConfig,
                                               NamespaceInternalKey searchKey, NameSpaceContainer existingContainer) throws NamespaceException {
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
            datasetConfig.setVersion(currentConfig.getVersion());
          }
        }
        break;
        case FOLDER:
          // delete the folder as it is being converted to a dataset
          namespace.delete(searchKey.getKey(), existingContainer.getFolder().getVersion());
          break;

        default:
          return false;
      }
    }

    try {
      addOrUpdateDataset(datasetPath, datasetConfig);
    } catch (ConcurrentModificationException e) {
      logger.warn("Failure while updating dataset " + datasetPath, e);
      // No one is checking the return value. TODO DX-4490
      return false;
    }

    return true;
  }

  @Override
  public Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> find(FindByCondition condition) {
    return Iterables.transform(condition == null ? namespace.find() : namespace.find(condition),
        new Function<Map.Entry<byte[], NameSpaceContainer>, Map.Entry<NamespaceKey, NameSpaceContainer>>() {
          @Override
          public Map.Entry<NamespaceKey, NameSpaceContainer> apply(Map.Entry<byte[], NameSpaceContainer> input) {
            return new AbstractMap.SimpleEntry<>(
                new NamespaceKey(input.getValue().getFullPathList()), input.getValue());
          }
        });
  }

  @Override
  public Iterable<Map.Entry<DatasetSplitId, DatasetSplit>> findSplits(FindByCondition condition) {
    return splitsStore.find(condition);
  }

  @Override
  public Iterable<Entry<DatasetSplitId, DatasetSplit>> findSplits(FindByRange<DatasetSplitId> range) {
    return splitsStore.find(range);
  }

  @Override
  public int getSplitCount(FindByCondition condition) {
    return splitsStore.getCounts(condition.getCondition()).get(0);
  }

  @Override
  public String dumpSplits() {
    try {
      StringBuilder builder = new StringBuilder();
      for (Map.Entry<DatasetSplitId, DatasetSplit> split : splitsStore.find()) {
        builder.append(format("%s: %s\n", split.getKey().toString(), split.getValue().toString()));
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
      final Iterable<Map.Entry<byte[], NameSpaceContainer>> containers = namespace.find(new FindByRange<>(
        NamespaceInternalKey.getRootLookupStart(), false, NamespaceInternalKey.getRootLookupEnd(), false));

      final List<NamespaceInternalKey> sourcesKeys = new ArrayList<>();
      final List<NamespaceInternalKey> spacesKeys = new ArrayList<>();
      final List<NamespaceInternalKey> homeKeys = new ArrayList<>();

      for (Map.Entry<byte[], NameSpaceContainer> entry : containers) {
        try {
          Type type = entry.getValue().getType();
          switch (type) {
            case HOME:
              homeKeys.add(NamespaceInternalKey.parseKey(entry.getKey()));
              break;
            case SOURCE:
              sourcesKeys.add(NamespaceInternalKey.parseKey(entry.getKey()));
              break;
            case SPACE:
              spacesKeys.add(NamespaceInternalKey.parseKey(entry.getKey()));
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

    final List<byte[]> keys = Lists.newArrayListWithExpectedSize(entityPath.getPathComponents().size());

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

    final IndexedStore.FindByCondition condition = new IndexedStore.FindByCondition()
        .setOffset(0)
        .setLimit(1)
        .setCondition(query);

    final Iterable<Entry<byte[], NameSpaceContainer>> result = namespace.find(condition);
    final Iterator<Entry<byte[], NameSpaceContainer>> it = result.iterator();
    return it.hasNext()?it.next().getValue():null;
  }

}

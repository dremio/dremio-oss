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
package com.dremio.service.embedded.catalog;

import static com.dremio.service.embedded.catalog.EmbeddedPointerStore.asNamespaceId;
import static com.google.common.base.Preconditions.checkArgument;

import com.dremio.datastore.api.KVStoreProvider;
import com.google.common.base.Suppliers;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.jetbrains.annotations.NotNull;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.ReferenceHistory;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.RelativeCommitSpec;
import org.projectnessie.versioned.RepositoryInformation;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.paging.FilteringPaginationIterator;
import org.projectnessie.versioned.paging.PaginationIterator;

/** An implementation of Nessie's {@link VersionStore} interface without commit log. */
public class EmbeddedUnversionedStore implements VersionStore {

  private static final Hash NO_ANCESTOR = Hash.of("11223344556677889900");
  private static final String DEFAULT_BRANCH_NAME = "main";
  private static final ReferenceInfo<CommitMeta> MAIN =
      ReferenceInfo.of(NO_ANCESTOR, BranchName.of(DEFAULT_BRANCH_NAME));

  private final Semaphore commitSemaphore = new Semaphore(1, true);
  private final Supplier<EmbeddedPointerStore> store;

  public EmbeddedUnversionedStore(Provider<KVStoreProvider> kvStoreProvider) {
    store = Suppliers.memoize(() -> new EmbeddedPointerStore(kvStoreProvider.get()));
  }

  @NotNull
  private static IdentifiedContentKey asIdentifiedContentKey(ContentKey key, Content content) {
    // The table's ID is taken for the `content` object. Namespaces get IDs generated based on they
    // ContentKey.
    return IdentifiedContentKey.identifiedContentKeyFromContent(
        key, content, elements -> asNamespaceId(ContentKey.of(elements)));
  }

  @NotNull
  @Override
  public Hash noAncestorHash() {
    return NO_ANCESTOR;
  }

  @Override
  public CommitResult<Commit> commit(
      @NotNull BranchName branch,
      @NotNull Optional<Hash> referenceHash,
      @NotNull CommitMeta metadata,
      @NotNull List<Operation> operations,
      @NotNull VersionStore.CommitValidator validator,
      @NotNull BiConsumer<ContentKey, String> addedContents)
      throws ReferenceNotFoundException, ReferenceConflictException {

    if (!DEFAULT_BRANCH_NAME.equals(branch.getName())) {
      throw new ReferenceNotFoundException("Invalid branch name: " + branch.getName());
    }

    try {
      commitSemaphore.acquire();
      try {
        checkOperationsForConflicts(operations);
        commitUnversioned(operations);
      } finally {
        commitSemaphore.release();
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }

    return CommitResult.<Commit>builder()
        .targetBranch(BranchName.of(DEFAULT_BRANCH_NAME))
        .commit(
            Commit.builder().commitMeta(metadata).hash(NO_ANCESTOR).parentHash(NO_ANCESTOR).build())
        .build();
  }

  private void checkOperationsForConflicts(List<Operation> operations)
      throws ReferenceConflictException {
    List<Conflict> conflicts = new ArrayList<>();
    for (Operation op : operations) {
      ContentKey key = op.getKey();
      String msgPrefix = "Key '" + key + "' ";
      if (op instanceof Put) {
        Content value = ((Put) op).getValue();
        if (value instanceof IcebergTable) {
          // detect and fail concurrent changes
          Content previous = store.get().get(key);
          if (previous == null) {
            if (value.getId() != null) {
              conflicts.add(
                  Conflict.conflict(
                      Conflict.ConflictType.KEY_DOES_NOT_EXIST,
                      key,
                      msgPrefix + "Cannot update a non-existing table"));
            }
          } else {
            if (value.getId() == null) {
              conflicts.add(
                  Conflict.conflict(
                      Conflict.ConflictType.KEY_EXISTS, key, msgPrefix + "Table already exists"));
            } else {
              String oldId = previous.getId();
              if (oldId != null && !oldId.equals(value.getId())) {
                conflicts.add(
                    Conflict.conflict(
                        Conflict.ConflictType.CONTENT_ID_DIFFERS,
                        key,
                        msgPrefix
                            + "Table content ID mismatch - expected: "
                            + value.getId()
                            + ", found: "
                            + oldId));
              }
            }
          }
        }
      }
    }
    if (!conflicts.isEmpty()) {
      throw new ReferenceConflictException(conflicts);
    }
  }

  private void commitUnversioned(List<Operation> operations) {
    for (Operation op : operations) {
      ContentKey key = op.getKey();
      if (op instanceof Put) {
        Content value = ((Put) op).getValue();
        store.get().put(key, value);
      } else if (op instanceof Delete) {
        store.get().delete(key);
      }
    }
  }

  @Override
  public ContentResult getValue(Ref ref, ContentKey key, boolean returnNotFound)
      throws ReferenceNotFoundException {
    return getValues(ref, Collections.singletonList(key), returnNotFound).get(key);
  }

  @Override
  public Map<ContentKey, ContentResult> getValues(
      Ref ref, Collection<ContentKey> keys, boolean returnNotFound) {
    // Note: `returnNotFound` is not supported due to lack of "Embedded" use cases for it.
    checkArgument(!returnNotFound, "returnNotFound==true is not supported for embedded use cases.");
    Map<ContentKey, ContentResult> result = new HashMap<>();
    keys.forEach(
        key -> {
          Content content = store.get().get(key);
          if (content != null) {
            result.put(
                key,
                ContentResult.contentResult(asIdentifiedContentKey(key, content), content, null));
          }
        });
    return result;
  }

  @Override
  public PaginationIterator<KeyEntry> getKeys(
      Ref ref, String pagingToken, boolean withContent, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    List<KeyEntry> entries =
        store
            .get()
            .findAll()
            .map(
                doc ->
                    KeyEntry.of(
                        asIdentifiedContentKey(doc.getKey(), doc.getValue()), doc.getValue()))
            .collect(Collectors.toList());
    return new SimpleNonPagedIterator<>(entries);
  }

  @Override
  public ReferenceInfo<CommitMeta> getNamedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    if (DEFAULT_BRANCH_NAME.equals(ref)) {
      return MAIN;
    }

    throw new ReferenceNotFoundException("Reference not found: " + ref);
  }

  @Override
  public PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) {
    return new SimpleNonPagedIterator<>(Collections.singletonList(MAIN));
  }

  @NotNull
  @Override
  public RepositoryInformation getRepositoryInformation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Hash hashOnReference(
      NamedRef namedReference,
      Optional<Hash> hashOnReference,
      List<RelativeCommitSpec> relativeLookups) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RepositoryConfig> getRepositoryConfig(
      Set<RepositoryConfig.Type> repositoryConfigTypes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RepositoryConfig updateRepositoryConfig(RepositoryConfig repositoryConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MergeResult<Commit> transplant(TransplantOp transplantOp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MergeResult<Commit> merge(MergeOp mergeOp)
      throws ReferenceNotFoundException, ReferenceConflictException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReferenceAssignedResult assign(NamedRef ref, Hash expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {

    throw new UnsupportedOperationException();
  }

  @Override
  public ReferenceCreatedResult create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {

    throw new UnsupportedOperationException();
  }

  @Override
  public ReferenceDeletedResult delete(NamedRef ref, Hash hash)
      throws ReferenceNotFoundException, ReferenceConflictException {

    throw new UnsupportedOperationException();
  }

  @Override
  public ReferenceHistory getReferenceHistory(String refName, Integer headCommitsToScan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PaginationIterator<Commit> getCommits(Ref ref, boolean fetchAdditionalInfo) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<IdentifiedContentKey> getIdentifiedKeys(Ref ref, Collection<ContentKey> keys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PaginationIterator<Diff> getDiffs(
      Ref from, Ref to, String pagingToken, KeyRestrictions keyRestrictions) {
    throw new UnsupportedOperationException();
  }

  private static class SimpleNonPagedIterator<T> extends FilteringPaginationIterator<T, T> {
    public SimpleNonPagedIterator(Collection<T> items) {
      super(items.iterator(), x -> x);
    }

    @Override
    protected String computeTokenForCurrent() {
      throw new IllegalArgumentException("Paging not supported by the storage model in use");
    }

    @Override
    public String tokenForEntry(T entry) {
      throw new IllegalArgumentException("Paging not supported by the storage model in use");
    }
  }
}

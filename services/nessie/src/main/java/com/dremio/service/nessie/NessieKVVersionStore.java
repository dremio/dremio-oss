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
package com.dremio.service.nessie;

import static com.dremio.service.nessie.Commit.NO_ANCESTOR;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.WithHash;

import com.dremio.common.util.Retryer;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.VersionOption;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MoreCollectors;
import com.google.common.collect.Streams;

/**
 * KVStore implementation of the Nessie VersionStore
 */
public final class NessieKVVersionStore implements VersionStore<Contents, CommitMeta> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NessieKVVersionStore.class);

  private final KVStore<Hash, NessieCommit> commits;
  private final KVStore<NamedRef, Hash> namedReferences;
  private final Serializer<Contents> valueSerializer;
  private final Serializer<CommitMeta> metadataSerializer;
  private final Supplier<Retryer<Void>> retrySupplier;

  /**
   * Builder class for a KVStoreVersionStore
   */
  public static final class Builder {
    private KVStore<Hash, NessieCommit> commits = null;
    private KVStore<NamedRef, Hash> namedReferences = null;
    private Serializer<Contents> valueSerializer = null;
    private Serializer<CommitMeta> metadataSerializer = null;
    private String defaultBranchName = null;
    private Supplier<Integer> maxCommitRetriesSupplier = null;

    public NessieKVVersionStore.Builder commits(KVStore<Hash, NessieCommit> commits) {
      this.commits = commits;
      return this;
    }

    public NessieKVVersionStore.Builder namedReferences(KVStore<NamedRef, Hash> namedReferences) {
      this.namedReferences = namedReferences;
      return this;
    }

    public NessieKVVersionStore.Builder valueSerializer(Serializer<Contents> serializer) {
      this.valueSerializer = requireNonNull(serializer);
      return this;
    }

    public NessieKVVersionStore.Builder metadataSerializer(Serializer<CommitMeta> serializer) {
      this.metadataSerializer = requireNonNull(serializer);
      return this;
    }

    public NessieKVVersionStore.Builder defaultBranchName(String defaultBranchName) {
      this.defaultBranchName = defaultBranchName;
      return this;
    }

    public NessieKVVersionStore.Builder maxCommitRetriesSupplier(Supplier<Integer> maxCommitRetriesSupplier) {
      this.maxCommitRetriesSupplier = maxCommitRetriesSupplier;
      return this;
    }

    /**
     * Build a instance of the memory store.
     * @return a memory store instance
     */
    public NessieKVVersionStore build() {
      checkState(this.valueSerializer != null, "Value serializer hasn't been set");
      checkState(this.metadataSerializer != null, "Metadata serializer hasn't been set");
      checkState(this.commits != null, "Commits KVStore hasn't been set");
      checkState(this.namedReferences != null, "NamedRefs KVStore hasn't been set");
      checkState(this.defaultBranchName != null, "Default branch name hasn't been set");
      checkState(this.maxCommitRetriesSupplier != null, "Max commit retries name hasn't been set");
      return new NessieKVVersionStore(this);
    }
  }

  private NessieKVVersionStore(NessieKVVersionStore.Builder builder) {
    this.commits = builder.commits;
    this.namedReferences = builder.namedReferences;
    this.valueSerializer = builder.valueSerializer;
    this.metadataSerializer = builder.metadataSerializer;

    this.retrySupplier = () -> new Retryer.Builder()
      .setMaxRetries(builder.maxCommitRetriesSupplier.get())
      .retryIfExceptionOfType(ConcurrentModificationException.class)
      .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, 250, 2500)
      .build();

    final BranchName defaultBranch = BranchName.of(builder.defaultBranchName);
    try {
      toHash(defaultBranch);
    } catch (ReferenceNotFoundException e) {
      try {
        create(BranchName.of(builder.defaultBranchName), Optional.empty());
      } catch (ReferenceAlreadyExistsException raee) {
        // Already exists, continue
      }
    }
  }

  /**
   * Create a new Nessie KVstore builder.
   *
   * @return a builder for a Nessie KVstore
   */
  public static NessieKVVersionStore.Builder builder() {
    return new NessieKVVersionStore.Builder();
  }

  @Override
  public Hash toHash(NamedRef ref) throws ReferenceNotFoundException {
    logger.debug("toHash (ref: {})", ref);
    final Document<NamedRef, Hash> entry = namedReferences.get(requireNonNull(ref));
    if (entry != null) {
      final Hash hash = entry.getValue();
      if (hash != null) {
        return hash;
      }
    }
    throw ReferenceNotFoundException.forReference(ref);
  }

  private Hash toHash(Ref ref) throws ReferenceNotFoundException {
    logger.debug("toHash (ref: {})", ref);
    if (ref instanceof NamedRef) {
      return toHash((NamedRef) ref);
    }

    if (ref instanceof Hash) {
      final Hash hash = (Hash) ref;
      if (!hash.equals(NO_ANCESTOR) && !commits.contains(hash)) {
        throw ReferenceNotFoundException.forReference(hash);
      }
      return hash;
    }
    throw new IllegalArgumentException(format("Unsupported reference type for ref %s", ref));
  }

  @Override
  public WithHash<Ref> toRef(String refOfUnknownType) throws ReferenceNotFoundException {
    logger.debug("toRef (ref: {})", refOfUnknownType);
    requireNonNull(refOfUnknownType);
    Optional<WithHash<Ref>> result = Stream.<Function<String, Ref>>of(TagName::of, BranchName::of, Hash::of)
      .map(f -> {
        try {
          final Ref ref = f.apply(refOfUnknownType);
          return WithHash.of(toHash(ref), ref);
        } catch (IllegalArgumentException | ReferenceNotFoundException e) {
          // ignored malformed or nonexistent reference
          return null;
        }
      })
      .filter(Objects::nonNull)
      .findFirst();
    return result.orElseThrow(() -> ReferenceNotFoundException.forReference(refOfUnknownType));
  }

  @Override
  public void commit(BranchName branch, Optional<Hash> referenceHash,
                     CommitMeta metadata, List<Operation<Contents>> operations) throws ReferenceNotFoundException, ReferenceConflictException {
    final List<Key> keys = operations.stream().map(Operation::getKey).distinct().collect(Collectors.toList());
    logger.debug("commit (branch: {}, referenceHash: {}, keys: {})", branch, referenceHash, keys);
    final long overallStartTime = System.currentTimeMillis();

    // Retrying this block in case it fails due to a concurrent modification exception
    final Retryer<Void> newRetryer = retrySupplier.get();
    final AtomicInteger iteration = new AtomicInteger(1);
    try {
      newRetryer.call(() -> {
        // Validating commit hash
        final Hash currentHash = toHash(branch);
        checkConcurrentModification(branch, currentHash, referenceHash, keys);

        compute(namedReferences, branch, (key, hash) -> {
          // Persisting commit
          final NessieCommit commit = NessieCommit.of(valueSerializer, metadataSerializer, currentHash, metadata, operations);
          final Hash previousHash = Optional.ofNullable(hash).orElse(Commit.NO_ANCESTOR);
          if (!previousHash.equals(currentHash)) {
            // A new change was committed concurrently
            throw new ConcurrentModificationException("A new change was committed concurrently on branch/tag " + branch);
          }
          // Duplicates are very unlikely and also okay to ignore
          final Hash commitHash = commit.getHash();
          final Document<Hash, ? extends Commit<Contents, CommitMeta>> commitEntry = commits.get(commitHash);
          final long startTime = System.currentTimeMillis();
          if (commitEntry != null) {
            commits.put(commitHash, commit, VersionOption.from(commitEntry));
          } else {
            commits.put(commitHash, commit, KVStore.PutOption.CREATE);
          }
          final long endTime = System.currentTimeMillis();
          logger.debug("commit (branch: {}, referenceHash: {}, keys: {}) succeeded. Took {}ms and {} attempts.",
            branch, referenceHash, keys, endTime - startTime, iteration.getAndIncrement());
          return commitHash;
        });
        final long overallTime = System.currentTimeMillis() - overallStartTime;
        logger.debug("NessieKVVersionStore commit() took {}ms total", overallTime);
        if (overallTime > 1000L) {
          logger.info("NessieKVVersionStore commit() took longer than 1s: {}ms total", overallTime);
        }
        return null;
      });
    } catch (Retryer.OperationFailedAfterRetriesException e) {
      logger.error("commit operation failed after reaching max retries ({}) and took {}ms total to try all attempts.",
        newRetryer.getMaxRetries(), System.currentTimeMillis() - overallStartTime);
      if (e.getCause() instanceof ReferenceNotFoundException) {
        throw (ReferenceNotFoundException) e.getCause();
      } else if (e.getCause() instanceof ReferenceConflictException) {
        throw (ReferenceConflictException) e.getCause();
      } else if (e.getCause() instanceof ConcurrentModificationException) {
        throw new ReferenceConflictException(e.getCause().getMessage(), e.getCause());
      } else if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException)e.getCause();
      }
    }
  }

  @Override
  public void transplant(BranchName targetBranch,
                         Optional<Hash> referenceHash,
                         List<Hash> sequenceToTransplant) throws ReferenceNotFoundException, ReferenceConflictException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void merge(Hash fromHash,
                    BranchName toBranch,
                    Optional<Hash> expectedHash) throws ReferenceNotFoundException, ReferenceConflictException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void assign(NamedRef ref,
                     Optional<Hash> expectedHash,
                     Hash targetHash) throws ReferenceNotFoundException, ReferenceConflictException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void create(NamedRef ref, Optional<Hash> targetHash) throws ReferenceAlreadyExistsException {
    logger.debug("create (ref: {}, targetHash: {})", ref, targetHash);
    Preconditions.checkArgument(ref instanceof BranchName || targetHash.isPresent(), "Cannot create an unassigned tag reference");

    compute(namedReferences, ref, (key, currentHash) -> {
      if (currentHash != null) {
        throw ReferenceAlreadyExistsException.forReference(ref);
      }

      return targetHash.orElse(Commit.NO_ANCESTOR);
    });
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash) throws ReferenceNotFoundException, ReferenceConflictException {
    logger.debug("delete (ref: {}, hash: {})", ref, hash);
    try {
      compute(namedReferences, ref, (key, currentHash) -> {
        if (currentHash == null) {
          throw ReferenceNotFoundException.forReference(ref);
        }

        ifPresent(hash, h -> {
          if (!h.equals(currentHash)) {
            throw ReferenceConflictException.forReference(ref, hash, (Optional.of(currentHash)));
          }
        });

        return null;
      });
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw e;
    } catch (VersionStoreException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private NessieCommit getCommitsValue(Hash key) {
    logger.debug("getCommitsValue (hash: {})", key);
    final Document<Hash, NessieCommit> entry = commits.get(key);
    if (entry != null) {
      return entry.getValue();
    }
    return null;
  }

  @Override
  public Stream<WithHash<CommitMeta>> getCommits(Ref ref) throws ReferenceNotFoundException {
    logger.debug("getCommits (ref: {})", ref);
    final Hash hash = toHash(ref);

    final Iterator<WithHash<Commit<Contents, CommitMeta>>> iterator = new CommitsIterator<>(this::getCommitsValue, hash);
    return Streams.stream(iterator).map(wh -> WithHash.of(wh.getHash(), wh.getValue().getMetadata()));
  }

  @Override
  public Stream<Key> getKeys(Ref ref) throws ReferenceNotFoundException {
    logger.debug("getKeys (ref: {})", ref);
    final Hash hash = toHash(ref);

    final Iterator<WithHash<Commit<Contents, CommitMeta>>> iterator = new CommitsIterator<>(this::getCommitsValue, hash);
    final Set<Key> deleted = new HashSet<>();
    return Streams.stream(iterator)
      // flatten the operations (in reverse order)
      .flatMap(wh -> Lists.reverse(wh.getValue().getOperations()).stream())
      // block deleted keys
      .filter(operation -> {
        Key key = operation.getKey();
        if (operation instanceof Delete) {
          deleted.add(key);
        }
        return !deleted.contains(key);
      })
      // extract the keys
      .map(Operation::getKey)
      // filter keys which have been seen already
      .distinct();
  }

  @Override
  public Contents getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    logger.debug("getValue (ref: {}, key: {})", ref, key);
    return getValues(ref, Collections.singletonList(key)).get(0).orElse(null);
  }

  @Override
  public List<Optional<Contents>> getValues(Ref ref, List<Key> keys) throws ReferenceNotFoundException {
    logger.debug("getValues (ref: {}, keys: {})", ref, keys);
    final Hash hash = toHash(ref);

    final int size = keys.size();
    final List<Optional<Contents>> results = new ArrayList<>(size);
    results.addAll(Collections.nCopies(size, Optional.empty()));

    final Set<Key> toFind = new HashSet<>(keys);

    final Iterator<WithHash<Commit<Contents, CommitMeta>>> iterator = new CommitsIterator<>(this::getCommitsValue, hash);
    while (iterator.hasNext()) {
      if (toFind.isEmpty()) {
        // early exit if all keys have been found
        break;
      }

      final Commit<Contents, CommitMeta> commit = iterator.next().getValue();
      for (Operation<Contents> operation : Lists.reverse(commit.getOperations())) {
        final Key operationKey = operation.getKey();
        // ignore keys of no interest
        if (!toFind.contains(operationKey)) {
          continue;
        }

        if (operation instanceof Put) {
          final Put<Contents> put = (Put<Contents>) operation;
          int index = keys.indexOf(operationKey);
          results.set(index, Optional.of(put.getValue()));
          toFind.remove(operationKey);
        } else if (operation instanceof Delete) {
          // No need to fill with Optional.empty() as the results were pre-filled
          toFind.remove(operationKey);
        } else if (operation instanceof Unchanged) {
          continue;
        } else {
          throw new AssertionError("Unsupported operation type for " + operation);
        }
      }
    }
    return results;
  }

  @Override
  public Stream<Diff<Contents>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Collector collectGarbage() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @VisibleForTesting
  Supplier<Retryer<Void>> getRetrySupplier() {
    return retrySupplier;
  }

  private void checkValidReferenceHash(BranchName branch, Hash currentBranchHash, Hash referenceHash)
    throws ReferenceNotFoundException {
    if (referenceHash.equals(Commit.NO_ANCESTOR)) {
      return;
    }
    final Optional<Hash> foundHash = Streams.stream(new CommitsIterator<>(this::getCommitsValue, currentBranchHash))
      .map(WithHash::getHash)
      .filter(hash -> hash.equals(referenceHash))
      .collect(MoreCollectors.toOptional());

    foundHash.orElseThrow(() -> new ReferenceNotFoundException(format("'%s' hash is not a valid commit from branch '%s'(%s)",
      referenceHash, branch, currentBranchHash)));
  }

  private void checkConcurrentModification(final BranchName targetBranch, final Hash currentHash, final Optional<Hash> referenceHash,
                                           final List<Key> keyList) throws ReferenceNotFoundException, ReferenceConflictException {
    logger.debug("checkConcurrentModification (targetBranch: {}, currentHash: {}, referenceHash: {}, keyList: {})",
      targetBranch, currentHash, referenceHash, keyList);

    // Validate commit
    try {
      ifPresent(referenceHash, hash -> {
        checkValidReferenceHash(targetBranch, currentHash, hash);

        final List<Optional<Contents>> referenceValues = getValues(hash, keyList);
        final List<Optional<Contents>> currentValues = getValues(currentHash, keyList);
        logger.debug("checkConcurrentModification (referenceValues: {}, currentValues: {})", referenceValues, currentValues);

        if (!referenceValues.equals(currentValues)) {
          throw ReferenceConflictException.forReference(targetBranch, referenceHash, Optional.of(currentHash));
        }
      });
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw e;
    } catch (VersionStoreException e) {
      throw new AssertionError(e);
    }
  }

  @SuppressWarnings("serial")
  private static final class VersionStoreExecutionError extends Error {
    private VersionStoreExecutionError(VersionStoreException cause) {
      super(cause);
    }

    @Override
    public synchronized VersionStoreException getCause() {
      return (VersionStoreException) super.getCause();
    }
  }

  @FunctionalInterface
  private interface ComputeFunction<K, V, E extends VersionStoreException> {
    V apply(K k, V v) throws E;
  }

  @FunctionalInterface
  private interface IfPresentConsumer<V, E extends VersionStoreException> {
    void accept(V v) throws E;
  }

  private static <K, V, E extends VersionStoreException> void compute(KVStore<K, V> kvStore, K key, NessieKVVersionStore.ComputeFunction<K, V, E> doCompute)
    throws E {
    try {
      try {
        final Document<K, V> entry = kvStore.get(key);
        if (entry != null) {
          final V oldValue = entry.getValue();
          final V newValue = doCompute.apply(key, oldValue);
          if (newValue != null) {
            kvStore.put(key, newValue, VersionOption.from(entry));
          } else {
            kvStore.delete(key, VersionOption.from(entry));
          }
        } else {
          final V newValue = doCompute.apply(key, null);
          if (newValue != null) {
            kvStore.put(key, newValue, KVStore.PutOption.CREATE);
          }
        }
      } catch (VersionStoreException e) {
        // e is of type E but cannot catch a generic type
        throw new NessieKVVersionStore.VersionStoreExecutionError(e);
      }
    } catch (NessieKVVersionStore.VersionStoreExecutionError e) {
      @SuppressWarnings("unchecked")
      E cause = (E) e.getCause();
      throw cause;
    }
  }

  private static <T, E extends VersionStoreException> void ifPresent(Optional<T> optional, NessieKVVersionStore.IfPresentConsumer<? super T, E> consumer)
    throws E {
    try {
      optional.ifPresent(value -> {
        try {
          consumer.accept(value);
        } catch (VersionStoreException e) {
          // e is of type E but cannot catch a generic type
          throw new NessieKVVersionStore.VersionStoreExecutionError(e);
        }
      });
    } catch (NessieKVVersionStore.VersionStoreExecutionError e) {
      @SuppressWarnings("unchecked")
      E cause = (E) e.getCause();
      throw cause;
    }
  }
}

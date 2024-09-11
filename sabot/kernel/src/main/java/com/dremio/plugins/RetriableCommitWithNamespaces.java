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
package com.dremio.plugins;

import static org.projectnessie.model.ContentKey.MAX_ELEMENTS;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetriableCommitWithNamespaces {
  private static final Logger logger = LoggerFactory.getLogger(RetriableCommitWithNamespaces.class);

  private final NessieApiV2 nessieApi;
  private final Branch branch;
  private final CommitMeta commitMeta;
  private final Operation.Put operation;
  private final String logPrefix;
  private final Set<ContentKey> parentNamespacesToCreate = new HashSet<>();
  private NessieConflictException lastCommitException;

  public RetriableCommitWithNamespaces(
      NessieApiV2 nessieApi, Branch branch, CommitMeta commitMeta, Operation.Put operation) {
    this.nessieApi = nessieApi;
    this.branch = branch;
    this.commitMeta = commitMeta;
    this.operation = operation;
    this.logPrefix = String.format("Nessie commit of %s to %s", operation, branch);
  }

  public Branch commit() throws NessieConflictException {
    int retryCount = 0;
    // note that before https://github.com/projectnessie/nessie/pull/7704 missing parent namespaces
    // were being reported 1 by 1, so we keep using MAX_ELEMENTS here
    while (retryCount < MAX_ELEMENTS) {
      Optional<Branch> maybeBranchHead = this.tryCommit();
      if (maybeBranchHead.isPresent()) {
        Branch branchHead = maybeBranchHead.get();
        logger.info(
            "{} succeeded as {} (retries: {}, createdNamespaces: {})",
            logPrefix,
            branchHead,
            retryCount,
            parentNamespacesToCreate);
        return branchHead;
      }
      retryCount += 1;
    }
    throw this.getLastConflictException();
  }

  @VisibleForTesting
  Optional<Branch> tryCommit() throws NessieConflictException {
    CommitMultipleOperationsBuilder commitBuilder =
        nessieApi.commitMultipleOperations().branch(branch).commitMeta(commitMeta);
    commitBuilder.operation(operation);
    for (ContentKey parentNamespace : parentNamespacesToCreate) {
      commitBuilder.operation(Operation.Put.of(parentNamespace, Namespace.of(parentNamespace)));
    }
    try {
      return Optional.of(commitBuilder.commit());
    } catch (NessieReferenceNotFoundException e) {
      logger.error("{} failed due to Reference not found", logPrefix, e);
      throw new ReferenceNotFoundException(e);
    } catch (NessieNotFoundException e) {
      logger.error("{} failed due to Nessie not found", logPrefix, e);
      throw UserException.dataReadError(e).buildSilently();
    } catch (NessieConflictException e) {
      lastCommitException = e;
      if (e.getErrorDetails() instanceof ReferenceConflicts) {
        ReferenceConflicts referenceConflicts = (ReferenceConflicts) e.getErrorDetails();
        List<Conflict> fatalConflicts = handleNamespaceConflicts(referenceConflicts.conflicts());
        if (fatalConflicts.isEmpty() && !parentNamespacesToCreate.isEmpty()) {
          logger.info(
              "{} failed due to missing parent namespaces. Retry is possible with namespace creation: {}",
              logPrefix,
              parentNamespacesToCreate);
          return Optional.empty();
        }
        logger.error("{} failed with unrecoverable conflicts: {}", logPrefix, fatalConflicts, e);
        throw e;
      }
      logger.error("{} failed with unknown conflict", logPrefix, e);
      throw e;
    }
  }

  private List<Conflict> handleNamespaceConflicts(List<Conflict> commitConflicts) {
    List<Conflict> fatalConflicts = new ArrayList<>();
    for (Conflict conflict : commitConflicts) {
      ContentKey currentConflictKey = conflict.key();
      switch (conflict.conflictType()) {
        case NAMESPACE_ABSENT:
          if (!operation.getKey().startsWith(currentConflictKey)) {
            throw new IllegalStateException(
                String.format(
                    "%s NAMESPACE_ABSENT but %s is not a parent of %s",
                    logPrefix, currentConflictKey, operation.getKey()));
          }
          parentNamespacesToCreate.add(currentConflictKey);
          break;
        case KEY_EXISTS:
          if (operation.getKey().equals(currentConflictKey)) {
            // a conflict for the primary operation can not be retried by this class
            fatalConflicts.add(conflict);
          } else {
            // another commit created this parent namespace while we were retrying
            boolean removed = parentNamespacesToCreate.remove(currentConflictKey);
            if (!removed) {
              throw new IllegalStateException(
                  String.format(
                      "%s KEY_EXISTS but %s was not in parentNamespacesToCreate",
                      logPrefix, currentConflictKey));
            }
          }
          break;
        default:
          // we are unable to retry other conflict types
          fatalConflicts.add(conflict);
          break;
      }
    }
    return fatalConflicts;
  }

  @VisibleForTesting
  Set<ContentKey> getParentNamespacesToCreate() {
    return parentNamespacesToCreate;
  }

  @VisibleForTesting
  NessieConflictException getLastConflictException() {
    return lastCommitException;
  }
}

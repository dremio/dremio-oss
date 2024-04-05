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
import com.dremio.exec.store.NessieNamespaceAlreadyExistsException;
import com.dremio.exec.store.ReferenceNotFoundException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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
  private final Operation topLevelOperation;
  private final Map<ContentKey, Operation> operations = new HashMap<>();
  private NessieConflictException lastCommitException;

  public RetriableCommitWithNamespaces(
      NessieApiV2 nessieApi, Branch branch, CommitMeta commitMeta, Operation topLevelOperation) {
    this.nessieApi = nessieApi;
    this.branch = branch;
    this.commitMeta = commitMeta;
    this.topLevelOperation = topLevelOperation;
    operations.put(topLevelOperation.getKey(), topLevelOperation);
  }

  public void commit() throws NessieConflictException {
    int retryCount = 0;
    while (retryCount < MAX_ELEMENTS) {
      if (this.tryCommit()) {
        return;
      }
      retryCount += 1;
    }
    throw this.getLastConflictException();
  }

  public boolean tryCommit() throws NessieConflictException {
    CommitMultipleOperationsBuilder commitMultipleOperationsBuilder =
        nessieApi.commitMultipleOperations().branch(branch).commitMeta(commitMeta);
    for (Operation op : operations.values()) {
      commitMultipleOperationsBuilder.operation(op);
    }
    try {
      commitMultipleOperationsBuilder.commit();
      return true;
    } catch (org.projectnessie.error.NessieNamespaceAlreadyExistsException e) {
      logger.error("Failed to create folder as folder already exists", e);
      throw new NessieNamespaceAlreadyExistsException(e);
    } catch (NessieReferenceNotFoundException e) {
      logger.error("Failed to create folder due to Reference not found", e);
      throw new ReferenceNotFoundException(e);
    } catch (NessieConflictException e) {
      logger.warn("Failed to create folder due to Nessie conflict", e);
      lastCommitException = e;
      for (Conflict conflict : ((ReferenceConflicts) e.getErrorDetails()).conflicts()) {
        ContentKey currentConflictKey = conflict.key();
        switch (conflict.conflictType()) {
          case NAMESPACE_ABSENT:
            operations.put(
                currentConflictKey,
                Operation.Put.of(currentConflictKey, Namespace.of(currentConflictKey)));
            break;
          case KEY_EXISTS:
            // case for if only the original operation is still failing with key exists conflict.
            if (currentConflictKey.compareTo(topLevelOperation.getKey()) == 0) {
              throw e;
            }
            operations.remove(currentConflictKey);
            break;
          default:
            throw e;
        }
      }
    } catch (NessieNotFoundException e) {
      logger.error("Failed to create folder due to Nessie not found", e);
      throw UserException.dataReadError(e).buildSilently();
    }
    return false;
  }

  public Collection<Operation> getOperations() {
    return operations.values();
  }

  public NessieConflictException getLastConflictException() {
    return lastCommitException;
  }
}

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

import static com.dremio.config.DremioConfig.NESSIE_SERVICE_REMOTE_URI;

import org.projectnessie.client.api.AssignBranchBuilder;
import org.projectnessie.client.api.AssignTagBuilder;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.CreateReferenceBuilder;
import org.projectnessie.client.api.DeleteBranchBuilder;
import org.projectnessie.client.api.DeleteTagBuilder;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetContentsBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.GetReferenceBuilder;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.NessieConfiguration;

/**
 * Implementation of NessieAPIV1 that always throws UnsupportedOperationException.
 * For use when Nessie integration is not configured.
 */
public class NessieApiV1Unsupported implements NessieApiV1 {
  @Override
  public NessieConfiguration getConfig() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public GetContentsBuilder getContents() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public GetAllReferencesBuilder getAllReferences() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public CreateReferenceBuilder createReference() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public GetReferenceBuilder getReference() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public GetEntriesBuilder getEntries() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public GetCommitLogBuilder getCommitLog() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public AssignTagBuilder assignTag() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public DeleteTagBuilder deleteTag() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public AssignBranchBuilder assignBranch() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public DeleteBranchBuilder deleteBranch() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public TransplantCommitsBuilder transplantCommitsIntoBranch() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public MergeReferenceBuilder mergeRefIntoBranch() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public CommitMultipleOperationsBuilder commitMultipleOperations() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException(String.format("Nessie integration is disabled. %s was not configured.", NESSIE_SERVICE_REMOTE_URI));
  }
}

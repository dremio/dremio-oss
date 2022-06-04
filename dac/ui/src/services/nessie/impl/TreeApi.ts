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

import moize from 'moize';
import {
  DefaultApi,
  GetAllReferencesRequest,
  GetReferenceByNameRequest,
  GetCommitLogRequest,
  GetEntriesRequest,
  CreateReferenceRequest,
  DeleteBranchRequest,
  MergeRefIntoBranchRequest
} from '../client';
import SwaggerConfig, { createSwaggerConfig } from './SwaggerConfig';

//Use default Atlantis project API
const TreeApi = new DefaultApi(SwaggerConfig);

//Get and cache (moize) endpoint-specific API (empty endpoint = default Atlantis API)
export const getTreeApi = moize(function(endpoint?: string) {
  return new DefaultApi(createSwaggerConfig(endpoint));
}, {
  maxSize: 10
});

export function getDefaultBranch() {
  return TreeApi.getDefaultBranch();
}

export function getAllReferences(requestParameters: GetAllReferencesRequest) {
  return TreeApi.getAllReferences(requestParameters);
}

export function getReferenceByName(req: GetReferenceByNameRequest) {
  return TreeApi.getReferenceByName(req);
}

export function getCommitLog(req: GetCommitLogRequest) {
  return TreeApi.getCommitLog(req);
}

export function getEntries(req: GetEntriesRequest) {
  return TreeApi.getEntries(req);
}

export function createReference(req: CreateReferenceRequest) {
  return TreeApi.createReference(req);
}

export function deleteReference(req: DeleteBranchRequest) {
  return TreeApi.deleteBranch(req);
}

export function mergeReference(req: MergeRefIntoBranchRequest) {
  return TreeApi.mergeRefIntoBranch(req);
}

export default TreeApi;

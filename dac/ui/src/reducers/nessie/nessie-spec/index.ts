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
import { initialState } from "../nessie";
import { NessieRootState } from "@app/types/nessie";

export const empty = { ...initialState };

export const testStateEmpty = {
  nessie: empty,
  nessie2: empty,
};

export const tagState = {
  defaultReference: {
    type: "BRANCH",
    name: "main",
    hash: "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d",
  },
  reference: {
    type: "TAG",
    name: "new-tag--7874853884837871442",
    hash: "83d76c1f11a718bc703a38bc8386b6284bee5ad49850dff99fec2f1ce43fef2e",
  },
  hash: null,
  date: null,
  loading: {},
  errors: {},
};

export const commitState = {
  defaultReference: {
    type: "BRANCH",
    name: "main",
    hash: "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d",
  },
  reference: {
    type: "BRANCH",
    name: "main",
    hash: "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d",
  },
  hash: "83d76c1f11a718bc703a38bc8386b6284bee5ad49850dff99fec2f1ce43fef2e",
  date: null,
  loading: {},
  errors: {},
};
export const branchState = {
  defaultReference: {
    type: "BRANCH",
    name: "main",
    hash: "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d",
  },
  reference: {
    type: "BRANCH",
    name: "main",
    hash: "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d",
  },
  hash: null,
  date: null,
  loading: {},
  errors: {},
};

export const populatedState = {
  arctic_source: commitState,
  arctic_source1: tagState,
  arctic_source3: branchState,
  "ref/arctic_source": branchState,
  "ref/arctic_source1": branchState,
  "ref/arctic_source3": branchState,
} as NessieRootState;

export const datasetPayload = {
  arctic_source: {
    type: "COMMIT",
    value: "83d76c1f11a718bc703a38bc8386b6284bee5ad49850dff99fec2f1ce43fef2e",
  },
  arctic_source1: { type: "TAG", value: "new-tag--7874853884837871442" },
  arctic_source3: { type: "BRANCH", value: "main" },
};

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
import { Branch, Tag } from "@app/services/nessie/client";

// Map - sourceId: NessieState
export type NessieRootState = {
  [key: string]: NessieState;
};

type TagType = { type: "TAG" } & Tag;
type BranchType = { type: "BRANCH" } & Branch;
export type Reference = TagType | BranchType;

export type NessieState = {
  defaultReference: Reference | null;
  reference: Reference | null;
  hash: string | null;
  date: Date | null; // When user selects a commit before certain time in branch picker
  loading: { [key: string]: boolean };
  errors: { [key: string]: any };
};

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

import type { CommunityDataset } from "./Dataset.js";
import type { File } from "../community/catalog/File.js";
import type { Folder } from "./Folder.js";
import type { CommunitySource } from "./Source.js";
import type { Home } from "../community/catalog/Home.js";
import type { VersionedDataset } from "../community/catalog/VersionedDataset.js";
import type { Space } from "../community/catalog/Space.js";
import type { CatalogReference } from "./CatalogReference.js";
import type { CatalogFunction } from "../community/catalog/CatalogFunction.js";

export type CatalogObjectMethods = {
  get name(): string;
  get path(): string[];
  pathString(SEPARATOR?: string): string;
  referenceType: string;
  children?: () => {
    data(): AsyncGenerator<CatalogReference, void, undefined>;
  };
};

export type CatalogObject =
  | CatalogFunction
  | CommunityDataset
  | File
  | Folder
  | CommunitySource
  | Home
  | VersionedDataset
  | Space;

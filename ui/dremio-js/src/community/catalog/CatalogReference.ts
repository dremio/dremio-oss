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
import type {
  CatalogReference as CatalogReferenceInterface,
  CatalogReferenceProperties,
} from "../../interfaces/CatalogReference.js";
import { pathString } from "./utils.js";

export class CatalogReference implements CatalogReferenceInterface {
  readonly id: string;
  readonly path: string[];
  readonly type:
    | `DATASET_${"DIRECT" | "PROMOTED" | "VIRTUAL"}`
    | "FILE"
    | "FOLDER"
    | "HOME"
    | "SOURCE"
    | "SPACE"
    | "FUNCTION";

  #retrieve: any;

  constructor(properties: CatalogReferenceProperties, retrieve: any) {
    this.id = properties.id;
    this.path = properties.path;
    this.type = properties.type;
    this.#retrieve = retrieve;
  }

  pathString = pathString(() => this.path);

  async resolve() {
    return this.#retrieve(this.id);
  }
}

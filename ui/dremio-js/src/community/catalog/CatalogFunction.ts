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
  Function as FunctionInterface,
  FunctionProperties,
} from "../../interfaces/Function.js";
import type { FunctionCatalogReference } from "./CatalogReference.js";

import { pathString } from "./utils.js";

export class CatalogFunction implements FunctionInterface {
  readonly catalogReference: FunctionCatalogReference;
  readonly createdAt: FunctionProperties["createdAt"];
  /**
   * @deprecated
   */
  readonly id: FunctionProperties["id"];
  readonly isScalar: FunctionProperties["isScalar"];
  readonly lastModified: FunctionProperties["lastModified"];
  /**
   * @deprecated
   */
  readonly path: FunctionProperties["path"];
  readonly returnType: FunctionProperties["returnType"];
  // eslint-disable-next-line no-unused-private-class-members
  readonly #tag: string;

  constructor(
    properties: FunctionProperties & {
      catalogReference: FunctionCatalogReference;
      tag: string;
    },
  ) {
    this.catalogReference = properties.catalogReference;
    this.createdAt = properties.createdAt;
    this.id = properties.id;
    this.isScalar = properties.isScalar;
    this.lastModified = properties.lastModified;
    this.path = properties.path;
    this.returnType = properties.returnType;
    this.#tag = properties.tag;
  }

  /**
   * @deprecated
   */
  get name(): string {
    return this.path[this.path.length - 1]!;
  }

  /**
   * @deprecated
   */
  get referenceType() {
    return "FUNCTION" as const;
  }

  /**
   * @deprecated
   */
  pathString = pathString(() => this.path);
}

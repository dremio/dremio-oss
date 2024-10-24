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
import type { SonarV3Config } from "../../_internal/types/Config.js";
import type {
  Folder as FolderInterface,
  FolderProperties,
} from "../../interfaces/Folder.js";
import { FolderCatalogReference } from "./CatalogReference.js";
import { catalogReferenceFromProperties } from "./catalogReferenceFromProperties.js";
import { catalogReferenceEntityToProperties, pathString } from "./utils.js";

export class Folder implements FolderInterface {
  readonly catalogReference: FolderCatalogReference;
  readonly createdAt: FolderInterface["createdAt"];
  /**
   * @deprecated
   */
  readonly id: FolderInterface["id"];
  /**
   * @deprecated
   */
  readonly path: FolderInterface["path"];

  #children: ReturnType<typeof catalogReferenceFromProperties>[];

  constructor(
    properties: FolderProperties & {
      catalogReference: FolderCatalogReference;
      children: ReturnType<typeof catalogReferenceFromProperties>[];
    },
  ) {
    this.catalogReference = properties.catalogReference;
    this.#children = properties.children;
    this.createdAt = properties.createdAt;
    this.id = properties.id;
    this.path = properties.path;
  }

  children() {
    const c = this.#children;
    return {
      async *data() {
        yield* c;
      },
    };
  }

  /**
   * @deprecated
   */
  get name() {
    return this.path[this.path.length - 1]!;
  }

  /**
   * @deprecated
   */
  pathString = pathString(() => this.path);

  /**
   * @deprecated
   */
  get referenceType() {
    return "FOLDER" as const;
  }

  static fromResource(properties: any, config: SonarV3Config) {
    return new Folder({
      catalogReference: new FolderCatalogReference(
        {
          id: properties.id,
          path: properties.path,
        },
        config,
      ),
      children: properties.children.map((child: any) =>
        catalogReferenceFromProperties(
          catalogReferenceEntityToProperties(child),
          config,
        ),
      ),
      createdAt: new Date(properties.createdAt),
      id: properties.id,
      path: properties.path,
    });
  }
}

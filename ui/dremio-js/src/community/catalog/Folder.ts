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
  Folder as FolderInterface,
  FolderProperties,
} from "../../interfaces/Folder.js";
import { CatalogReference } from "./CatalogReference.js";
import { catalogReferenceEntityToProperties, pathString } from "./utils.js";

export class Folder implements FolderInterface {
  readonly createdAt: FolderInterface["createdAt"];
  readonly id: FolderInterface["id"];
  readonly path: FolderInterface["path"];

  #children: CatalogReference[];

  constructor(properties: FolderProperties & { children: CatalogReference[] }) {
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

  get name() {
    return this.path[this.path.length - 1]!;
  }

  pathString = pathString(() => this.path);

  static fromResource(properties: any, retrieve: any) {
    return new Folder({
      children: properties.children.map(
        (child: any) =>
          new CatalogReference(
            catalogReferenceEntityToProperties(child),
            retrieve,
          ),
      ),
      createdAt: new Date(properties.createdAt),
      id: properties.id,
      path: properties.path,
    });
  }
}

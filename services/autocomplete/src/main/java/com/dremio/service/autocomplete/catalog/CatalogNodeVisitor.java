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
package com.dremio.service.autocomplete.catalog;

/**
 * Visitor for a CatalogNode
 * @param <R> the return type.
 */
public interface CatalogNodeVisitor<R> {
  R visit(DatasetCatalogNode datasetCatalogNode);

  R visit(FileCatalogNode fileCatalogNode);

  R visit(FolderCatalogNode folderCatalogNode);

  R visit(HomeCatalogNode homeCatalogNode);

  R visit(SourceCatalogNode sourceCatalogNode);

  R visit(SpaceCatalogNode spaceCatalogNode);
}

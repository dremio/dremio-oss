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

import { memo, Suspense, type FC } from "react";
import { ErrorBoundary } from "react-error-boundary";
import type { CatalogReference } from "@dremio/dremio-js/interfaces";
import { CatalogReferenceChild } from "./CatalogReferenceChild";
import { FailedCatalogTreeItem } from "./FailedCatalogTreeItem";
import { LoadingCatalogTreeItem } from "./LoadingCatalogTreeItem";

const renderCatalogReferenceSkeleton = (catalogReference: CatalogReference) => (
  <LoadingCatalogTreeItem key={catalogReference.id} />
);

const renderCatalogReferenceChild = (catalogReference: CatalogReference) => (
  <ErrorBoundary
    fallback={<FailedCatalogTreeItem catalogReference={catalogReference} />}
    key={catalogReference.id}
  >
    <CatalogReferenceChild catalogReference={catalogReference} />
  </ErrorBoundary>
);

export const CatalogTreeChildren: FC<{
  catalogReferences: CatalogReference[];
}> = memo(function CatalogTreeChildren(props) {
  return (
    <Suspense
      fallback={props.catalogReferences.map(renderCatalogReferenceSkeleton)}
    >
      {props.catalogReferences.map(renderCatalogReferenceChild)}
    </Suspense>
  );
});

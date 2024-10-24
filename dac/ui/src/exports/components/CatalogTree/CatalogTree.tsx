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

import { memo, type FC } from "react";
import type { CatalogReference } from "@dremio/dremio-js/interfaces";
import { ExpandedContextProvider, useTree } from "../Tree";
import { CatalogTreeChildren } from "./CatalogTreeChildren";

export const CatalogTree: FC<{
  catalogReferences: CatalogReference[];
}> = memo(function CatalogTree(props) {
  const treeProps = useTree({
    childrenIds: props.catalogReferences.map((ref) => ref.id),
  });

  return (
    <ExpandedContextProvider>
      <div className="tree px-105 pt-05 overflow-y-auto" {...treeProps}>
        <CatalogTreeChildren catalogReferences={props.catalogReferences} />
      </div>
    </ExpandedContextProvider>
  );
});
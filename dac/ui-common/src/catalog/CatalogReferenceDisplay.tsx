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

import { CatalogReference } from "@dremio/dremio-js/interfaces";
import { CatalogReferenceIcon } from "./CatalogReferenceIcon";
import { FC } from "react";
import { HighlightIndices } from "../components/HighlightIndices";

export const CatalogReferenceDisplay: FC<{
  catalogReference: CatalogReference;
  highlightIndices?: {
    name?: number[];
    pathString: number[];
  };
}> = (props) => (
  <div className="flex flex-row items-center gap-1">
    <CatalogReferenceIcon catalogReference={props.catalogReference} />
    <div className="flex flex-col gap-05">
      <div className="text-semibold">
        <HighlightIndices indices={props.highlightIndices?.name}>
          {props.catalogReference.name}
        </HighlightIndices>
      </div>
      <div className="text-sm dremio-typography-less-important">
        <HighlightIndices indices={props.highlightIndices?.pathString}>
          {props.catalogReference.pathString(".")}
        </HighlightIndices>
      </div>
    </div>
  </div>
);

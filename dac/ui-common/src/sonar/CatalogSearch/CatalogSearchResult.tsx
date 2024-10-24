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
// @ts-nocheck

import {
  CatalogReference,
  CommunityUser,
  Role,
} from "@dremio/dremio-js/interfaces";
import { FC, useMemo } from "react";
import { CatalogReferenceDisplay } from "../../catalog/CatalogReferenceDisplay";
import { DremioUser } from "../../components/DremioUser";
import { formatRelativeTimeFromNow } from "../../utilities/formatRelativeTimeFromNow";
import fuzzysort from "fuzzysort";
import { HighlightIndices } from "../../components/HighlightIndices";

const CatalogResultDetails: FC<{ owner: CommunityUser | Role }> = (props) => {
  return <DremioUser user={props.owner} />;
};

const higlightableKeys = [
  "name",
  "description",
  "pathString",
  "ownerName",
] as const;

export const CatalogSearchResult: FC<{
  catalogReference: CatalogReference;
  lastModifiedAt: Date;
  owner?: CommunityUser | Role;
  description: string | null;
  searchText: string;
}> = (props) => {
  const highlightableFields: Record<keyof typeof higlightableKeys, string> =
    useMemo(() => {
      return {
        name: props.catalogReference.name,
        description: props.description,
        pathString: props.catalogReference.pathString("."),
        ownerName: props.owner?.displayName,
      };
    }, [props.catalogReference, props.description, props.owner]);
  const results = useMemo(() => {
    const result = fuzzysort.go(props.searchText, [highlightableFields], {
      keys: higlightableKeys,
      threshold: 0.4,
    });
    if (result.length !== 1) {
      return {};
    }
    const [name, description, pathString, ownerName] = result[0];
    return {
      name: name.indexes,
      description: description.indexes,
      pathString: pathString.indexes,
      ownerName: ownerName.indexes,
    };
  }, [highlightableFields, props.searchText]);
  return (
    <div
      className="bg-primary border-thin p-2 hover"
      style={{ borderRadius: "4px" }}
    >
      <div className="flex flex-col gap-2">
        <div className="flex flex-col">
          <CatalogReferenceDisplay
            catalogReference={props.catalogReference}
            highlightIndices={{
              name: results.name,
              pathString: results.pathString,
            }}
          />
        </div>
        {highlightableFields.description &&
        highlightableFields.description.length > 0 ? (
          <div className="text-sm" style={{ lineHeight: "1.5" }}>
            <HighlightIndices indices={results.description}>
              {highlightableFields.description}
            </HighlightIndices>
          </div>
        ) : null}
        <div className="text-sm flex flex-row gap-1 dremio-typography-less-important items-center">
          <div>
            Last updated {formatRelativeTimeFromNow(props.lastModifiedAt)}
          </div>
          {props.owner ? (
            <>
              &bull; <CatalogResultDetails owner={props.owner} />
            </>
          ) : null}
        </div>
      </div>
    </div>
  );
};

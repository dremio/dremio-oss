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

import { useEffect, useRef } from "react";
import { SmartResource } from "smart-resource";
import { useResourceSnapshot, useResourceStatus } from "smart-resource/react";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { FetchOption } from "@app/services/nessie/client";

export const useArcticCatalogTags = (searchFilter: string) => {
  const { apiV2 } = useNessieContext();

  const ArcticCatalogTagsResource = useRef(
    new SmartResource(
      ({ search }) =>
        apiV2.getAllReferencesV2({
          fetch: FetchOption.All,
          filter: search
            ? `refType == 'TAG' && ref.name.matches('(?i)${search}')`
            : "refType == 'TAG'",
          // endpoint doesn't respect the maxRecords param, virtual rendering/infinite scroll will be used for now
          // maxRecords: 10,
        }),
      { mode: "takeEvery" }
    )
  );

  useEffect(() => {
    ArcticCatalogTagsResource.current.fetch({ search: searchFilter });
  }, [searchFilter]);

  return [
    ...useResourceSnapshot(ArcticCatalogTagsResource.current),
    useResourceStatus(ArcticCatalogTagsResource.current),
    ArcticCatalogTagsResource.current.fetch.bind(
      ArcticCatalogTagsResource.current
    ),
  ] as const;
};

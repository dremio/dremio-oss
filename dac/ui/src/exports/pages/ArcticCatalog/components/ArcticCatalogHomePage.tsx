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
import NessieHomePage from "@app/pages/NessieHomePage/NessieHomePage";
import { getArcticProjectUrl } from "@app/utils/nessieUtils";
import * as PATHS from "@app/exports/paths";

function ArcticCatalogHomePage(props: any) {
  return (
    <NessieHomePage
      {...props}
      source={{
        name: props.catalog?.name,
        id: props.arcticCatalogId,
        endpoint: getArcticProjectUrl(props.arcticCatalogId),
        endpointV1: getArcticProjectUrl(props.arcticCatalogId, "v1"),
      }}
      baseUrl={PATHS.arcticCatalogBase({
        arcticCatalogId: props.arcticCatalogId,
      })}
    />
  );
}
export default ArcticCatalogHomePage;

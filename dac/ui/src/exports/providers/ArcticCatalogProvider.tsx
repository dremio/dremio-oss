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
import { getArcticCatalogResource } from "../resources/ArcticCatalogResource";
import { useResourceSnapshot } from "smart-resource/react";
import { type ArcticCatalog } from "../endpoints/ArcticCatalogs/ArcticCatalog.type";
import { withRouter } from "react-router";

type Props = {
  params: {
    arcticCatalogId: string;
  };
  children: ({
    catalog,
    catalogErr,
  }: {
    catalog: ArcticCatalog | null;
    catalogErr: unknown;
  }) => JSX.Element;
};

export const ArcticCatalogProvider = withRouter((props: Props): JSX.Element => {
  const catalogResource = useRef(
    getArcticCatalogResource(props.params.arcticCatalogId)
  );
  catalogResource.current = getArcticCatalogResource(
    props.params.arcticCatalogId
  );
  const [catalog, catalogErr] = useResourceSnapshot(catalogResource.current);
  useEffect(() => {
    catalogResource.current.fetch();
  }, [props.params.arcticCatalogId]);

  return props.children({ catalog, catalogErr });
});

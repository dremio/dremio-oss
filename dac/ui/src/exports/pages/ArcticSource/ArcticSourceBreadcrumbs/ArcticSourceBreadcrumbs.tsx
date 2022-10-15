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

import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { Link } from "react-router";
import {
  constructArcticUrl,
  useArcticCatalogContext,
} from "../../ArcticCatalog/arctic-catalog-utils";

import * as classes from "./ArcticSourceBreadcrumbs.module.less";

const ArcticSourceBreadcrumbs = () => {
  const { source, baseUrl } = useNessieContext();
  const { reservedNamespace } = useArcticCatalogContext() ?? {};
  return (
    <>
      <p className={classes["arcticSourceBreadcrumbs"]}>
        <Link
          to={constructArcticUrl({
            type: "source",
            baseUrl,
            tab: "data",
            namespace: reservedNamespace ?? "",
          })}
        >
          {source.name}
        </Link>{" "}
        / History
      </p>
    </>
  );
};

export default ArcticSourceBreadcrumbs;

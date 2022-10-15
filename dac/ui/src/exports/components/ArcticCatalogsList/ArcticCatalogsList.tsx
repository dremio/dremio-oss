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

import clsx from "clsx";
import { Link } from "react-router";
import { type ArcticCatalog } from "../../endpoints/ArcticCatalogs/ArcticCatalog.type";
import { ArcticCatalogCard } from "../ArcticCatalogCard/ArcticCatalogCard";
import { ArcticCatalogCardSkeleton } from "../ArcticCatalogCard/ArcticCatalogCardSkeleton";
import * as PATHS from "../../paths";
import classes from "./ArcticCatalogsList.less";

type CatalogsListProps = {
  catalogs: ArcticCatalog[] | null;
};

const renderSkeletons = () => Array(6).fill(<ArcticCatalogCardSkeleton />);

export const ArcticCatalogsList = (props: CatalogsListProps): JSX.Element => {
  return (
    <ul className={clsx("dremio-layout-grid", classes["catalogs-list"])}>
      {props.catalogs
        ? props.catalogs.map((catalog) => (
            <li key={catalog.id}>
              <Link
                to={PATHS.arcticCatalog({
                  arcticCatalogId: catalog.id,
                })}
                className="no-underline"
              >
                <ArcticCatalogCard catalog={catalog} />
              </Link>
            </li>
          ))
        : renderSkeletons()}
    </ul>
  );
};

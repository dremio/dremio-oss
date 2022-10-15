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

import { useState } from "react";
import { type WithRouterProps } from "react-router";
import ArcticTableHeader from "@app/exports/components/ArcticTableHeader/ArcticTableHeader";
import ArcticCatalogTagsTable from "./components/ArcticCatalogTagsTable/ArcticCatalogTagsTable";
import { Spinner } from "dremio-ui-lib/dist-esm";
import { ArcticCatalogTabsType } from "../../ArcticCatalog";
import {
  constructArcticUrl,
  useArcticCatalogContext,
} from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { useArcticCatalogTags } from "./components/ArcticCatalogTagsTable/useArcticCatalogTags";
import { isSmartFetchLoading } from "@app/utils/isSmartFetchLoading";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { setReference } from "@app/actions/nessie/nessie";
import { useDispatch } from "react-redux";
import { Tag } from "@app/services/nessie/client";

import * as classes from "./ArcticCatalogTags.module.less";

type ArcticCatalogTagsProps = WithRouterProps;

function ArcticCatalogTags(props: ArcticCatalogTagsProps) {
  const { router } = props;
  const [searchFilter, setSearchFilter] = useState("");
  const { baseUrl, source } = useNessieContext();
  const { isCatalog } = useArcticCatalogContext() ?? {};
  const dispatch = useDispatch();

  const getPath = (tag: { type: "TAG" } & Tag, tab: ArcticCatalogTabsType) => {
    dispatch(setReference({ reference: tag }, source.name));
    return constructArcticUrl({
      type: isCatalog ? "catalog" : "source",
      baseUrl,
      tab,
      namespace: tag.name,
    });
  };

  const handlePush = (tag: { type: "TAG" } & Tag, tab: ArcticCatalogTabsType) =>
    router.push(getPath(tag, tab));

  const [data, , status, refetch] = useArcticCatalogTags(searchFilter);

  return (
    <div className={classes["arctic-catalog-tags"]}>
      <ArcticTableHeader
        placeholder="ArcticCatalog.Tags.SearchPlaceholder"
        onSearchChange={setSearchFilter}
      />
      {isSmartFetchLoading(status) ? (
        <Spinner className={classes["arctic-tags-spinner"]} />
      ) : (
        <ArcticCatalogTagsTable
          references={data?.references ?? []}
          refetch={() => refetch({ search: searchFilter })}
          handlePush={handlePush}
        />
      )}
    </div>
  );
}

export default ArcticCatalogTags;

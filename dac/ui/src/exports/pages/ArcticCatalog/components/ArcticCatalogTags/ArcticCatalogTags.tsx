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
import { useIntl } from "react-intl";

import * as classes from "./ArcticCatalogTags.module.less";

type ArcticCatalogTagsProps = WithRouterProps;

function ArcticCatalogTags(props: ArcticCatalogTagsProps) {
  const { formatMessage } = useIntl()
  const { router } = props;
  const [searchFilter, setSearchFilter] = useState("");
  const { baseUrl, source } = useNessieContext();
  const { isCatalog } = useArcticCatalogContext() ?? {};
  const dispatch = useDispatch();

  const getPath = (tab: ArcticCatalogTabsType, tag: { type: "TAG" } & Tag) =>
    constructArcticUrl({
      type: isCatalog ? "catalog" : "source",
      baseUrl,
      tab,
      namespace: tag.name,
    });

  const handleTabNavigation = (
    tab: ArcticCatalogTabsType,
    tag: { type: "TAG" } & Tag
  ) => {
    dispatch(setReference({ reference: tag }, source.name));
    return router.push(getPath(tab, tag));
  };

  const [data, , status, refetch] = useArcticCatalogTags(searchFilter);

  return (
    <div className={classes["arctic-catalog-tags"]}>
      <ArcticTableHeader
        placeholder="ArcticCatalog.Tags.SearchPlaceholder"
        onSearchChange={setSearchFilter}
        loading={!!data && isSmartFetchLoading(status)}
        name={formatMessage({ id: "ArcticCatalog.Tags.Header"})}
      />
      {!data && isSmartFetchLoading(status) ? (
        <Spinner className={classes["arctic-tags-spinner"]} />
      ) : (
        <ArcticCatalogTagsTable
          references={data?.references ?? []}
          refetch={() => refetch({ search: searchFilter })}
          goToDataTab={handleTabNavigation}
        />
      )}
    </div>
  );
}

export default ArcticCatalogTags;

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
import VersionedPageTableHeader from "@app/exports/pages/VersionedHomePage/components/VersionedPageTableHeader/VersionedPageTableHeader";
import VersionedPageTagsTable from "./components/VersionedPageTagsTable";
import { Spinner } from "dremio-ui-lib/components";
import { VersionedPageTabsType } from "@app/exports/pages/VersionedHomePage/VersionedHomePage";
import {
  constructVersionedEntityUrl,
  useVersionedPageContext,
} from "@app/exports/pages/VersionedHomePage/versioned-page-utils";
import { useVersionedPageTags } from "./components/useVersionedPageTags";
import { isSmartFetchLoading } from "@app/utils/isSmartFetchLoading";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { setReference } from "@app/actions/nessie/nessie";
import { useDispatch } from "react-redux";
import { Tag } from "@app/services/nessie/client";
import { useIntl } from "react-intl";

import * as classes from "./VersionedPageTags.module.less";

type VersionedPageTagsProps = WithRouterProps;

function VersionedPageTags(props: VersionedPageTagsProps) {
  const { formatMessage } = useIntl();
  const { router } = props;
  const [searchFilter, setSearchFilter] = useState("");
  const { baseUrl, stateKey } = useNessieContext();
  const { isCatalog } = useVersionedPageContext();
  const dispatch = useDispatch();

  const getPath = (tab: VersionedPageTabsType, tag: { type: "TAG" } & Tag) =>
    constructVersionedEntityUrl({
      type: isCatalog ? "catalog" : "source",
      baseUrl,
      tab,
      namespace: encodeURIComponent(tag.name),
    });

  const handleTabNavigation = (
    tab: VersionedPageTabsType,
    tag: { type: "TAG" } & Tag,
  ) => {
    dispatch(setReference({ reference: tag }, stateKey));
    return router.push(getPath(tab, tag));
  };

  const [data, , status, refetch] = useVersionedPageTags(searchFilter);

  return (
    <div className={classes["versioned-page-tags"]}>
      <VersionedPageTableHeader
        placeholder="VersionedEntity.Tags.SearchPlaceholder"
        onSearchChange={setSearchFilter}
        loading={!!data && isSmartFetchLoading(status)}
        name={formatMessage({ id: "VersionedEntity.Tags.Header" })}
      />
      {!data && isSmartFetchLoading(status) ? (
        <Spinner className={classes["versioned-page-tags-spinner"]} />
      ) : (
        <VersionedPageTagsTable
          references={data?.references ?? []}
          refetch={() => refetch({ search: searchFilter })}
          goToDataTab={handleTabNavigation}
        />
      )}
    </div>
  );
}

export default VersionedPageTags;

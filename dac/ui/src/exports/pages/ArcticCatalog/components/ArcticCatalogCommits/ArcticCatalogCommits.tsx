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

import { useMemo, useState } from "react";
import { useIntl } from "react-intl";
import { type WithRouterProps } from "react-router";
import PageBreadcrumbHeader from "@app/pages/NessieHomePage/components/PageBreadcrumbHeader/PageBreadcrumbHeader";
import ArcticCatalogCommitsTable from "./components/ArcticCatalogCommitsTable/ArcticCatalogCommitsTable";
import {
  constructArcticUrl,
  parseArcticCatalogUrl,
  useArcticCatalogContext,
} from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { Spinner } from "dremio-ui-lib/dist-esm";
import { SearchField } from "@app/components/Fields";
import { ArcticCatalogTabsType } from "@app/exports/pages/ArcticCatalog/ArcticCatalog";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { useArcticCatalogCommits } from "./useArcticCatalogCommits";
import { isSmartFetchLoading } from "@app/utils/isSmartFetchLoading";
import { debounce } from "lodash";
import { getGoToDataButton } from "./utils";

import * as classes from "./ArcticCatalogCommits.module.less";
import * as headerClasses from "@app/exports/components/ArcticTableHeader/ArcticTableHeader.module.less";

type ArcticCatalogCommitsProps = WithRouterProps;

function ArcticCatalogCommits(props: ArcticCatalogCommitsProps) {
  const intl = useIntl();
  const { router, params, location } = props;
  const [searchFilter, setSearchFilter] = useState("");
  const {
    state: { reference, hash },
    baseUrl,
  } = useNessieContext();
  const { isCatalog, reservedNamespace } = useArcticCatalogContext() ?? {};

  const getPath = (tab: ArcticCatalogTabsType) => {
    return constructArcticUrl({
      type: isCatalog ? "catalog" : "source",
      baseUrl,
      tab,
      namespace: reservedNamespace ?? "",
      hash: hash ? `?hash=${hash}` : "",
    });
  };

  const handlePush = (tab: ArcticCatalogTabsType) => router.push(getPath(tab));

  const path = useMemo(() => {
    return parseArcticCatalogUrl(
      location.pathname,
      `${baseUrl}/commits/${params?.branchName}`,
      "commits",
      params?.branchName
    );
  }, [location, params, baseUrl]);

  const onRowClick = (rowId: Record<string, any>) => {
    const hash: string = rowId.rowData.id;

    const url = constructArcticUrl({
      type: isCatalog ? "catalog" : "source",
      baseUrl,
      tab: "commit",
      namespace: params?.branchName,
      commitId: hash,
    });

    router.push(url);
  };

  const [token, setToken] = useState<string | undefined>();
  const [data, , status] = useArcticCatalogCommits({
    filter: searchFilter,
    branch: reference?.name,
    pageToken: token,
  });

  function loadNextPage(index: number) {
    if (
      index > 0 &&
      data?.logEntries &&
      index === data.logEntries.length - 1 &&
      data?.pageToken
    ) {
      setToken(data.pageToken);
    }
  }

  const debounceSearch = debounce((val: string) => {
    setToken(undefined);
    setSearchFilter(val);
  }, 250);

  return (
    <div className={classes["arctic-catalog-commits"]}>
      <PageBreadcrumbHeader
        path={path}
        className={headerClasses["arctic-table-header"]}
        rightContent={
          <span className={headerClasses["arctic-table-header__right"]}>
            {getGoToDataButton(handlePush)}
            <SearchField
              placeholder={intl.formatMessage({
                id: "ArcticCatalog.Commits.SearchPlaceholder",
              })}
              onChange={debounceSearch}
              showCloseIcon
              showIcon
              className={headerClasses["arctic-search-box"]}
            />
          </span>
        }
      />
      <div className={classes["arctic-commits__content"]}>
        {!data?.logEntries?.length && isSmartFetchLoading(status) ? (
          <Spinner className={classes["arctic-commits-spinner"]} />
        ) : (
          <ArcticCatalogCommitsTable
            commits={data.logEntries}
            onRowClick={onRowClick}
            loadNextPage={loadNextPage}
          />
        )}
      </div>
    </div>
  );
}

export default ArcticCatalogCommits;

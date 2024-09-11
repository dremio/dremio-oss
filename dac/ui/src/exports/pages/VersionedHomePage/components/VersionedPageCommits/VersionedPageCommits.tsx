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
import VersionedPageCommitsTable from "./VersionedPageCommitsTable";
import {
  constructVersionedEntityUrl,
  parseVersionedEntityUrl,
  useVersionedPageContext,
} from "@app/exports/pages/VersionedHomePage/versioned-page-utils";
import { Spinner } from "dremio-ui-lib/components";
import { SearchField } from "@app/components/Fields";
import { VersionedPageTabsType } from "@app/exports/pages/VersionedHomePage/VersionedHomePage";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { useVersionedPageCommits } from "./utils/useVersionedPageCommits";
import { isSmartFetchLoading } from "@app/utils/isSmartFetchLoading";
import { debounce } from "lodash";
import { getGoToDataButton } from "./utils/utils";
import { LogEntryV2 as LogEntry } from "@app/services/nessie/client/index";
import { useDispatch } from "react-redux";
import { setReference } from "@app/actions/nessie/nessie";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";

import * as headerClasses from "@app/exports/pages/VersionedHomePage/components/VersionedPageTableHeader/VersionedPageTableHeader.module.less";
import * as classes from "./VersionedPageCommits.module.less";

type VersionedPageCommitsProps = WithRouterProps;

function VersionedPageCommits(props: VersionedPageCommitsProps) {
  const intl = useIntl();
  const { router, params, location } = props;
  const [searchFilter, setSearchFilter] = useState("");
  const {
    state: { reference, hash },
    baseUrl,
    stateKey,
  } = useNessieContext();
  const { isCatalog, reservedNamespace } = useVersionedPageContext();
  const dispatch = useDispatch();

  const getPath = (tab: VersionedPageTabsType, item?: LogEntry) => {
    const toHash = item?.commitMeta?.hash ?? hash ?? "";
    return constructVersionedEntityUrl({
      type: isCatalog ? "catalog" : "source",
      baseUrl,
      tab,
      namespace: reservedNamespace ?? "",
      hash: toHash ? `?hash=${toHash}` : "",
    });
  };

  const handleReference = (item: LogEntry) => {
    if (item.commitMeta?.hash && reference?.name) {
      dispatch(
        setReference(
          {
            reference: {
              type: "BRANCH",
              name: reference.name,
              hash: item.commitMeta.hash,
            },
            hash: item.commitMeta.hash,
          },
          stateKey,
        ),
      );
    }
  };

  const handleTabNavigation = (tab: VersionedPageTabsType, item?: LogEntry) => {
    if (item) handleReference(item);
    return router.push(getPath(tab, item));
  };

  const path = useMemo(() => {
    return parseVersionedEntityUrl(
      rmProjectBase(location.pathname) || "/",
      rmProjectBase(
        `${baseUrl}/commits/${encodeURIComponent(params?.branchName)}`,
      ),
      "commits",
      params?.branchName,
    );
  }, [location, params, baseUrl]);

  const getCommitLink = (id: string) => {
    return constructVersionedEntityUrl({
      type: isCatalog ? "catalog" : "source",
      baseUrl,
      tab: "commit",
      namespace: encodeURIComponent(params?.branchName),
      commitId: id,
    });
  };

  const [token, setToken] = useState<string | undefined>();
  const [data, , status] = useVersionedPageCommits({
    filter: searchFilter,
    branch: reference?.name,
    hash,
    pageToken: token,
  });

  function loadNextPage() {
    if (data?.logEntries && data?.pageToken) {
      setToken(data.pageToken);
    }
  }

  const debounceSearch = debounce((val: string) => {
    setToken(undefined);
    setSearchFilter(val);
  }, 250);

  const loading = isSmartFetchLoading(status);

  return (
    <div className={classes["versioned-page-commits"]}>
      <PageBreadcrumbHeader
        path={path}
        className={headerClasses["versioned-page-table-header"]}
        rightContent={
          <span className={headerClasses["versioned-page-table-header__right"]}>
            {getGoToDataButton(handleTabNavigation)}
            <SearchField
              placeholder={intl.formatMessage({
                id: "VersionedEntity.Commits.SearchPlaceholder",
              })}
              onChange={debounceSearch}
              showCloseIcon
              showIcon
              className={headerClasses["versioned-search-box"]}
              loading={data?.logEntries && loading}
            />
          </span>
        }
      />
      <div className={classes["versioned-commits__content"]}>
        {!data?.logEntries && loading ? (
          <Spinner className={classes["versioned-commits-spinner"]} />
        ) : (
          <VersionedPageCommitsTable
            commits={data.logEntries || []}
            getCommitLink={getCommitLink}
            loadNextPage={loadNextPage}
            goToDataTab={handleTabNavigation}
          />
        )}
      </div>
    </div>
  );
}

export default VersionedPageCommits;

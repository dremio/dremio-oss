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

import { Page } from "dremio-ui-lib/components";
import { useEffect, useMemo, useState } from "react";
import { withRouter, type WithRouterProps } from "react-router";
import NavCrumbs from "@inject/components/NavCrumbs/NavCrumbs";
// @ts-ignore
import {
  getVersionedTabFromPathname,
  constructVersionedEntityUrl,
  VersionedPageContext,
} from "./versioned-page-utils";

import { last } from "lodash";
import { useNessieContext } from "#oss/pages/NessieHomePage/utils/context";

import VersionedSourceBreadcrumbs from "../VersionedSource/VersionedSourceBreadcrumbs/VersionedSourceBreadcrumbs";
import { ErrorBoundary } from "#oss/components/ErrorBoundary/ErrorBoundary";
import { useIntl } from "react-intl";
import {
  DEFAULT_REF_REQUEST,
  SET_REF_REQUEST,
} from "#oss/actions/nessie/nessie";
import { NotFound } from "#oss/exports/components/ErrorViews/NotFound";
import { getTracingContext } from "dremio-ui-common/contexts/TracingContext.js";
import VersionedHomePageTabs from "./components/VersionedPageTabs/VersionedPageTabs";

import classes from "./VersionedHomePage.module.less";
import { PageTop } from "dremio-ui-common/components/PageTop.js";

export type VersionedPageTabsType =
  | "data"
  | "commits"
  | "tags"
  | "branches"
  | "settings"
  | "commit"
  | "jobs";

export const versionedPageTabs: VersionedPageTabsType[] = [
  "data",
  "commits",
  "tags",
  "branches",
  "settings",
  "commit",
  "jobs",
];

// Don't show tabs UI for these pages
const notInTabView: VersionedPageTabsType[] = ["settings", "commit", "jobs"];

type VersionedHomePageProps = {
  children: any;
  isCatalog: boolean;
  renderSideNav: () => JSX.Element;
} & WithRouterProps;

const VersionedHomePage = (props: VersionedHomePageProps): JSX.Element => {
  const {
    location: { pathname },
    params: { splat, branchName, versionedEntityId },
    router,
    children,
    isCatalog,
    renderSideNav,
  } = props;
  const {
    state: { errors, defaultReference, reference, hash },
    baseUrl,
  } = useNessieContext();
  const intl = useIntl();
  const [reservedNamespace, setReservedNamespace] = useState(
    branchName && splat
      ? `${branchName ? encodeURIComponent(branchName) : ""}${`/${splat}`}`
      : (splat ?? ""),
  );

  useEffect(() => {
    if (versionedEntityId === sessionStorage.getItem("newCatalogId")) {
      setTimeout(() => {
        getTracingContext().appEvent("versioned-catalog-creation");
      }, 2000);
    }
    sessionStorage.removeItem("newCatalogId");
  }, [versionedEntityId]);

  // useEffect will reroute if the url doesn't have branch/namespace
  useEffect(() => {
    const endOfUrl = last(pathname.split("/")) ?? "";
    const refName = reference?.name ?? "";
    if (refName && (endOfUrl === "data" || endOfUrl === "commits")) {
      router.replace(
        constructVersionedEntityUrl({
          type: isCatalog ? "catalog" : "source",
          baseUrl,
          tab: endOfUrl,
          namespace: encodeURIComponent(reference?.name) ?? "",
          hash: hash ? `?hash=${hash}` : "",
        }),
      );
    }
  }, [defaultReference, reference, hash, pathname, router, baseUrl, isCatalog]);

  const activeTab = useMemo(() => {
    const tab = getVersionedTabFromPathname(pathname);
    if ((tab === "data" || tab === "settings") && !isCatalog) return;
    else return tab;
  }, [pathname, isCatalog]);

  useEffect(() => {
    if ((activeTab === "data" || activeTab === "commits") && branchName) {
      setReservedNamespace(
        `${encodeURIComponent(branchName)}${splat ? `/${splat}` : ""}`,
      );
    }
  }, [activeTab, splat, branchName]);

  const isContentNotFound =
    errors[DEFAULT_REF_REQUEST] || errors[SET_REF_REQUEST] || !activeTab;

  const errorMessage = intl.formatMessage(
    { id: "Support.error.section" },
    {
      section: intl.formatMessage({
        id: `SectionLabel.Versioned.${isCatalog ? "catalog" : "source"}`,
      }),
    },
  );

  const versionedContext = useMemo(
    () => ({
      reservedNamespace: reservedNamespace,
      catalogId: versionedEntityId,
      activeTab: activeTab ?? versionedPageTabs[0],
      isCatalog: isCatalog, // TODO: better way to do this?
    }),
    [activeTab, isCatalog, reservedNamespace, versionedEntityId],
  );

  const VersionedContentWithErrorWrapper = (
    <ErrorBoundary title={errorMessage}>
      <Page
        className={
          !isCatalog ? classes["source__page"] : classes["catalog-page"]
        }
        header={
          isCatalog ? (
            <PageTop>
              <NavCrumbs />
            </PageTop>
          ) : (
            <VersionedSourceBreadcrumbs />
          )
        }
      >
        {isContentNotFound ? (
          <div className={classes["versionedHomePage__notFound"]}>
            <NotFound />
          </div>
        ) : (
          <div className={classes["versionedHomePage"]}>
            {notInTabView.includes(activeTab) ? (
              children
            ) : (
              <VersionedHomePageTabs>{children}</VersionedHomePageTabs>
            )}
          </div>
        )}
      </Page>
    </ErrorBoundary>
  );

  return (
    <VersionedPageContext.Provider value={versionedContext}>
      {isCatalog ? (
        <div className="page-content">
          {renderSideNav()}
          {VersionedContentWithErrorWrapper}
        </div>
      ) : (
        VersionedContentWithErrorWrapper
      )}
    </VersionedPageContext.Provider>
  );
};

export default VersionedHomePage;
export const VersionedHomePageWithRoute = withRouter(VersionedHomePage);

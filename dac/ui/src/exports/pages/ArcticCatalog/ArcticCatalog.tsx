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
import * as PATHS from "../../paths";
import NavCrumbs from "@inject/components/NavCrumbs/NavCrumbs";
// @ts-ignore
import {
  getArcticTabFromPathname,
  ArcticCatalogContext,
  constructArcticUrl,
} from "./arctic-catalog-utils";

import ArcticCatalogHomePage from "./components/ArcticCatalogHomePage";
import { last } from "lodash";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import ArcticCatalogTabs from "./components/ArcticCatalogTabs/ArcticCatalogTabs";

import classes from "./ArcticCatalog.module.less";
import ArcticSourceBreadcrumbs from "../ArcticSource/ArcticSourceBreadcrumbs/ArcticSourceBreadcrumbs";
import { ArcticSideNav } from "@app/exports/components/SideNav/ArcticSideNav";
import { ErrorBoundary } from "@app/components/ErrorBoundary/ErrorBoundary";
import { useIntl } from "react-intl";
import {
  DEFAULT_REF_REQUEST,
  SET_REF_REQUEST,
} from "@app/actions/nessie/nessie";
import { NotFound } from "@app/exports/components/ErrorViews/NotFound";
import { ArcticCatalogProvider } from "@app/exports/providers/ArcticCatalogProvider";
import { getTracingContext } from "dremio-ui-common/contexts/TracingContext.js";
import { ARCTIC_STATE_PREFIX } from "@app/constants/nessie";

export type ArcticCatalogTabsType =
  | "data"
  | "commits"
  | "tags"
  | "branches"
  | "settings"
  | "commit"
  | "jobs";

export const arcticCatalogTabs: ArcticCatalogTabsType[] = [
  "data",
  "commits",
  "tags",
  "branches",
  "settings",
  "commit",
  "jobs",
];

// Don't show tabs UI for these pages
const notInTabView: ArcticCatalogTabsType[] = ["settings", "commit", "jobs"];

type ArcticCatalogProps = {
  children: any;
} & WithRouterProps;

export const ArcticCatalog = (props: ArcticCatalogProps): JSX.Element => {
  const {
    location: { pathname },
    params: { splat, branchName, arcticCatalogId },
    router,
    children,
  } = props;
  const {
    state: { errors, defaultReference, reference, hash },
    baseUrl,
  } = useNessieContext();
  const intl = useIntl();
  const [reservedNamespace, setReservedNamespace] = useState(splat ?? "");
  const isCatalog = useMemo(
    () => baseUrl.startsWith(PATHS.arcticCatalogs()),
    [baseUrl]
  );

  useEffect(() => {
    if (arcticCatalogId === sessionStorage.getItem("newCatalogId")) {
      setTimeout(() => {
        getTracingContext().appEvent("arctic-catalog-creation");
      }, 2000);
    }
    sessionStorage.removeItem("newCatalogId");
  }, [arcticCatalogId]);

  // useEffect will reroute if the url doesn't have branch/namespace
  useEffect(() => {
    const endOfUrl = last(pathname.split("/")) ?? "";
    const refName = reference?.name ?? "";
    if (refName && (endOfUrl === "data" || endOfUrl === "commits")) {
      router.replace(
        constructArcticUrl({
          type: isCatalog ? "catalog" : "source",
          baseUrl,
          tab: endOfUrl,
          namespace: reference?.name ?? "",
          hash: hash ? `?hash=${hash}` : "",
        })
      );
    }
  }, [defaultReference, reference, hash, pathname, router, baseUrl, isCatalog]);

  const activeTab = useMemo(() => {
    const tab = getArcticTabFromPathname(pathname);
    if ((tab === "data" || tab === "settings") && !isCatalog) return;
    else return tab;
  }, [pathname, isCatalog]);

  useEffect(() => {
    if ((activeTab === "data" || activeTab === "commits") && branchName) {
      setReservedNamespace(`${branchName}${splat ? `/${splat}` : ""}`);
    }
  }, [activeTab, splat, branchName]);

  const isContentNotFound =
    errors[DEFAULT_REF_REQUEST] || errors[SET_REF_REQUEST] || !activeTab;

  const errorMessage = intl.formatMessage(
    { id: "Support.error.section" },
    {
      section: intl.formatMessage({
        id: `SectionLabel.arctic.${isCatalog ? "catalog" : "source"}`,
      }),
    }
  );

  const arcticContext = useMemo(
    () => ({
      reservedNamespace: reservedNamespace,
      activeTab: activeTab ?? arcticCatalogTabs[0],
      isCatalog: isCatalog, // TODO: better way to do this?
    }),
    [activeTab, isCatalog, reservedNamespace]
  );

  const ArcticContentWithErrorWrapper = (
    <ErrorBoundary title={errorMessage}>
      <Page
        className={!isCatalog ? classes["arcticSource__page"] : ""}
        header={isCatalog ? <NavCrumbs /> : <ArcticSourceBreadcrumbs />}
      >
        {isContentNotFound ? (
          <div className={classes["arcticCatalog__notFound"]}>
            <NotFound />
          </div>
        ) : (
          <div className={classes["arcticCatalog"]}>
            {notInTabView.includes(activeTab) ? (
              children
            ) : (
              <ArcticCatalogTabs>{children}</ArcticCatalogTabs>
            )}
          </div>
        )}
      </Page>
    </ErrorBoundary>
  );

  return (
    <ArcticCatalogContext.Provider value={arcticContext}>
      {isCatalog ? (
        <div className="page-content">
          <ArcticSideNav />
          {ArcticContentWithErrorWrapper}
        </div>
      ) : (
        ArcticContentWithErrorWrapper
      )}
    </ArcticCatalogContext.Provider>
  );
};

const ArcticCatalogWithRoute = withRouter(ArcticCatalog);
const ArcticCatalogWithNessie = ({ children, ...props }: any) => (
  <ArcticCatalogProvider>
    {(providerProps) => (
      <ArcticCatalogHomePage
        arcticCatalogId={props?.params?.arcticCatalogId}
        initialRef={{
          name: props?.params?.branchName,
          hash: props?.location?.query?.hash,
        }}
        statePrefix={ARCTIC_STATE_PREFIX}
        pathname={props.location.pathname}
        {...providerProps}
      >
        <ArcticCatalogWithRoute>{children}</ArcticCatalogWithRoute>
      </ArcticCatalogHomePage>
    )}
  </ArcticCatalogProvider>
);

export default ArcticCatalogWithNessie;

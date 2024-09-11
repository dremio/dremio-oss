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

import { createContext, useContext } from "react";
import { versionedPageTabs, VersionedPageTabsType } from "./VersionedHomePage";
import { Type } from "@app/types/nessie";
import * as PATHS from "../../paths";
import * as commonPaths from "dremio-ui-common/paths/common.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import {
  getVersionedUrlForCatalog,
  getUrlForVersionedSource,
} from "@inject/arctic/utils/arctic-catalog-utils";

type VersionedPageContextType = {
  reservedNamespace: string;
  catalogId: string;
  activeTab: VersionedPageTabsType;
  isCatalog: boolean;
};

export const VersionedPageContext =
  createContext<VersionedPageContextType | null>(null);

export function useVersionedPageContext(): VersionedPageContextType {
  const context = useContext(VersionedPageContext);
  return context as VersionedPageContextType;
}

export const getIconName = (tab: VersionedPageTabsType) => {
  switch (tab) {
    case "data":
      return "navigation-bar/dataset";
    case "commits":
      return "vcs/commit";
    case "tags":
      return "vcs/tag";
    case "branches":
      return "vcs/branch";
    default:
      return "navigation-bar/dataset";
  }
};

export function getTooltipIntlId(type?: string | null) {
  switch (type) {
    case Type.IcebergTable:
      return `Nessie.${type}`;
    case Type.DeltaLakeTable:
      return `Nessie.${type}`;
    case Type.IcebergView:
      return `Nessie.${type}`;
    default:
      return "Nessie.Namespace";
  }
}

export function getIconByType(type?: string | null) {
  switch (type) {
    case Type.IcebergTable:
      return "entities/iceberg-table";
    case Type.DeltaLakeTable:
      return "entities/dataset";
    case Type.IcebergView:
      return "entities/iceberg-view";
    default:
      return "entities/blue-folder";
  }
}

export function parseVersionedEntityUrl(
  url: string,
  path: string,
  tab: string,
  branchName: string,
) {
  if (url === "/") return undefined;
  else if (url.includes(`/${tab}/${encodeURIComponent(branchName)}/`)) {
    return url.replace(`${path}/`, "").split("/");
  } else {
    return [];
  }
}

export const getVersionedTabFromPathname = (pathname: string) => {
  const paths = pathname?.split?.("/") ?? [];
  const tabIdx = paths.findIndex(
    (path: string) =>
      versionedPageTabs.indexOf(path as VersionedPageTabsType) >= 0,
  );

  if (tabIdx < 0) {
    return;
  } else return paths[tabIdx] as VersionedPageTabsType;
};

export type VersionedEntityUrlProps = {
  type: "catalog" | "source";
  baseUrl: string;
  tab: VersionedPageTabsType;
  namespace: string;
  hash?: string;
  commitId?: string;
};

/**
 * Based on the versioned type (catalog or source), it will create a url that takes the user
 * to the correct location with the given params. Specifically, if user is in a versioned source, user will return
 * to the home page correctly. Otherwise, user will stay in versioned view.
 *
 * @param type provides which entity (catalog or source) to build the url for
 * @param baseUrl provides a base string for the url
 * @param tab provides the tab for the target location
 * @param namespace provides the location inside the data space, notated as `/:branchName` or `/:branchName/*`, where `*` is any nested folders
 * @param hash provides the hash the branch picker is on, notated as `?hash=:hashId`
 * @param commitId provides the commit hash for the commit details page
 */
export const constructVersionedEntityUrl = ({
  type = "catalog",
  baseUrl,
  tab,
  namespace = "",
  hash,
  commitId,
}: VersionedEntityUrlProps) => {
  const hashParam = hash ? `${hash}` : "";
  const base = baseUrl;
  const tabName = tab ? `/${tab}` : "";
  const space = namespace ? `/${namespace}` : "";

  const defaultPath = `${base}${tabName}${space}${hashParam}`;

  // if baseUrl is an empty string, use default concatenation
  if (baseUrl === "") {
    return defaultPath;
  } else {
    if (type === "catalog") {
      return getVersionedUrlForCatalog(
        baseUrl,
        tab,
        namespace,
        commitId,
        hashParam,
      );
    } else if (type === "source") {
      return getVersionedSourceTypeForUrl(
        baseUrl,
        tab,
        namespace,
        commitId,
        hashParam,
      );
    }
  }
  return defaultPath;
};

export const getVersionedSourceTypeForUrl = (
  baseUrl: string,
  tab: VersionedPageTabsType,
  namespace: string,
  commitId?: string,
  hash?: string,
) => {
  const basePath = rmProjectBase(baseUrl) || "/";
  const sourceVersion = basePath.split("/")[2];
  const isNessieSource = sourceVersion === "nessie";

  return isNessieSource
    ? getUrlForNessieSource(baseUrl, tab, namespace, commitId, hash)
    : getUrlForVersionedSource(baseUrl, tab, namespace, commitId, hash);
};

/**
 * Builds the url for traversing in an nessie source, or back to the source homepage
 **/
export const getUrlForNessieSource = (
  baseUrl: string,
  tab: VersionedPageTabsType,
  namespace: string,
  commitId?: string,
  hash?: string,
) => {
  const basePath = rmProjectBase(baseUrl) || "/";
  const sourceId = basePath.split("/")[3];
  const separatedNamespace = namespace.split("/");
  const branch = separatedNamespace.shift();
  const curNamespace = separatedNamespace.join("/");
  const projectId = getSonarContext()?.getSelectedProjectId?.();
  const prefix = PATHS.nessieSourceBase({ sourceId, projectId });

  switch (tab) {
    case "data": {
      // if "data", user should be taken to the original source homepage
      return `${commonPaths.source.link({
        resourceId: sourceId,
        projectId,
      })}${convertCatalogToSourceSpace(namespace ?? "")}`;
    }
    case "commits":
      if (!branch) {
        return `${prefix}/${PATHS.nessieSourceCommitsNonBase()}`;
      } else {
        return `${prefix}/${PATHS.nessieSourceCommits({
          branchId: branch,
          namespace: curNamespace,
        })}${hash}`;
      }
    case "commit":
      if (!branch || !commitId) {
        return `${prefix}/${PATHS.nessieSourceCommitsNonBase()}`;
      } else {
        return `${prefix}/${PATHS.nessieSourceCommit({
          branchId: branch,
          commitId: commitId,
        })}`;
      }
    case "tags":
      return `${prefix}/${PATHS.nessieSourceTagsNonBase()}`;
    case "branches":
      return `${prefix}/${PATHS.nessieSourceBranchesNonBase()}`;
    default:
      return `${prefix}`;
  }
};

/**
 * If user is looking at a versioned source, and is going to the source homepage from the source history,
 * this converts the namespace from `/:branchName/*` to `/folder/* `, where `*` is any nested folders
 **/
export const convertCatalogToSourceSpace = (namespace: string) => {
  const modifiedNamespace = namespace.split("/");
  modifiedNamespace.shift();
  const sourceNamespace = namespace ? modifiedNamespace.join("/") : namespace;
  return sourceNamespace ? `/folder/${sourceNamespace}` : sourceNamespace;
};

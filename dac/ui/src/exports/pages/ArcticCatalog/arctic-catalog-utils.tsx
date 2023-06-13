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
import { arcticCatalogTabs, ArcticCatalogTabsType } from "./ArcticCatalog";
import { Type } from "@app/types/nessie";
import * as PATHS from "../../paths";
import * as commonPaths from "dremio-ui-common/paths/common.js";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";

type ArcticCatalogContextType = {
  reservedNamespace: string;
  activeTab: ArcticCatalogTabsType;
  isCatalog: boolean;
};

export const ArcticCatalogContext =
  createContext<ArcticCatalogContextType | null>(null);

export function useArcticCatalogContext(): ArcticCatalogContextType | null {
  const context = useContext(ArcticCatalogContext);
  if (context === null) return null;
  return context as ArcticCatalogContextType;
}

export const getIconName = (tab: ArcticCatalogTabsType) => {
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

export function getIconByType(type?: string | null) {
  switch (type) {
    case Type.IcebergTable:
      return { type: "IcebergTable", id: `Nessie.${type}` };
    case Type.DeltaLakeTable:
      return { type: "PhysicalDataset", id: `Nessie.${type}` };
    case Type.IcebergView:
      return { type: "IcebergView", id: `Nessie.${type}` };
    default:
      return {
        type: "Folder",
        id: "Nessie.Namespace",
      };
  }
}

export const getArcticMessageId = (tab: ArcticCatalogTabsType) => {
  const pre = "ArcticCatalog";
  switch (tab) {
    case "data":
      return `${pre}.Data`;
    case "commits":
      return `${pre}.Commits`;
    case "tags":
      return `${pre}.Tags`;
    case "branches":
      return `${pre}.Branches`;
    default:
      return `${pre}.Data`;
  }
};

export function parseArcticCatalogUrl(
  url: string,
  path: string,
  tab: string,
  branchName: string
) {
  if (url === "/") return undefined;
  else if (url.includes(`/${tab}/${branchName}/`)) {
    return url.replace(`${path}/`, "").split("/");
  } else {
    return [];
  }
}

export const getArcticTabFromPathname = (pathname: string) => {
  const paths = pathname?.split?.("/") ?? [];
  const tabIdx = paths.findIndex(
    (path: string) =>
      arcticCatalogTabs.indexOf(path as ArcticCatalogTabsType) >= 0
  );

  if (tabIdx < 0) {
    return;
  } else return paths[tabIdx] as ArcticCatalogTabsType;
};

export type ArcticUrlProps = {
  type: "catalog" | "source";
  baseUrl: string;
  tab: ArcticCatalogTabsType;
  namespace: string;
  hash?: string;
  commitId?: string;
};

/**
 * Based on the arctic type (catalog or source), it will create a url that takes the user
 * to the correct location with the given params. Specifically, if user is in an Arctic Source, user will return
 * to the home page correctly. Otherwise, user will stay in arctic view.
 *
 * @param type provides which entity (catalog or source) to build the url for
 * @param baseUrl provides a base string for the url. Catalog is `/arctic/:catalogId`, source is `/sources/arctic/:sourceId`, and empty string uses default concatenation
 * @param tab provides the tab for the target location
 * @param namespace provides the location inside the data space, notated as `/:branchName` or `/:branchName/*`, where `*` is any nested folders
 * @param hash provides the hash the branch picker is on, notated as `?hash=:hashId`
 * @param commitId provides the commit hash for the commit details page
 */
export const constructArcticUrl = ({
  type = "catalog",
  baseUrl,
  tab,
  namespace = "",
  hash,
  commitId,
}: ArcticUrlProps) => {
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
      return getArcticUrlForCatalog(
        baseUrl,
        tab,
        namespace,
        commitId,
        hashParam
      );
    } else if (type === "source") {
      return getArcticUrlForSource(
        baseUrl,
        tab,
        namespace,
        commitId,
        hashParam
      );
    }
  }
  return defaultPath;
};

/**
 * Builds the url for traversing in an arctic catalog
 **/
export const getArcticUrlForCatalog = (
  baseUrl: string,
  tab: ArcticCatalogTabsType,
  namespace: string,
  commitId?: string,
  hash?: string
) => {
  const catalogId = baseUrl.split("/")[2];
  const separatedNamespace = namespace.split("/");
  const branch = separatedNamespace.shift();
  const curNamespace = separatedNamespace.join("/");

  switch (tab) {
    case "data": {
      if (!branch) {
        return PATHS.arcticCatalogDataBase({
          arcticCatalogId: catalogId,
        });
      } else {
        return `${PATHS.arcticCatalogData({
          arcticCatalogId: catalogId,
          branchId: branch,
          namespace: curNamespace,
        })}${hash}`;
      }
    }
    case "commits":
      if (!branch) {
        return PATHS.arcticCatalogCommitsBase({
          arcticCatalogId: catalogId,
        });
      } else {
        return `${PATHS.arcticCatalogCommits({
          arcticCatalogId: catalogId,
          branchId: branch,
          namespace: curNamespace,
        })}${hash}`;
      }
    case "tags":
      return PATHS.arcticCatalogTags({
        arcticCatalogId: catalogId,
      });
    case "branches":
      return PATHS.arcticCatalogBranches({
        arcticCatalogId: catalogId,
      });
    case "commit":
      if (!branch || !commitId) {
        return PATHS.arcticCatalogCommitsBase({
          arcticCatalogId: catalogId,
        });
      } else {
        return PATHS.arcticCatalogCommit({
          arcticCatalogId: catalogId,
          branchId: branch,
          commitId: commitId,
        });
      }
    case "settings":
      return PATHS.arcticCatalogSettings({
        arcticCatalogId: catalogId,
      });
    case "jobs":
      return PATHS.arcticCatalogJobs({
        arcticCatalogId: catalogId,
      });
    default:
      return PATHS.arcticCatalog({
        arcticCatalogId: catalogId,
      });
  }
};

/**
 * Builds the url for traversing in an arctic or nessie source, or back to the source homepage
 **/
export const getArcticUrlForSource = (
  baseUrl: string,
  tab: ArcticCatalogTabsType,
  namespace: string,
  commitId?: string,
  hash?: string
) => {
  const basePath = rmProjectBase(baseUrl) || "/";
  const sourceVersion = basePath.split("/")[2];
  const sourceId = basePath.split("/")[3];
  const separatedNamespace = namespace.split("/");
  const branch = separatedNamespace.shift();
  const curNamespace = separatedNamespace.join("/");
  const projectId = getSonarContext()?.getSelectedProjectId?.();
  const isNessieSource = sourceVersion === "nessie";
  const prefix = isNessieSource
    ? PATHS.nessieSourceBase({ sourceId, projectId })
    : PATHS.arcticSourceBase({ sourceId, projectId });

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
        return `${prefix}/${PATHS.arcticSourceCommitsNonBase()}`;
      } else {
        return `${prefix}/${PATHS.arcticSourceCommits({
          branchId: branch,
          namespace: curNamespace,
        })}${hash}`;
      }
    case "commit":
      if (!branch || !commitId) {
        return `${prefix}/${PATHS.arcticSourceCommitsNonBase()}`;
      } else {
        return `${prefix}/${PATHS.arcticSourceCommit({
          branchId: branch,
          commitId: commitId,
        })}`;
      }
    case "tags":
      return `${prefix}/${PATHS.arcticSourceTagsNonBase()}`;
    case "branches":
      return `${prefix}/${PATHS.arcticSourceBranchesNonBase()}`;
    default:
      return `${prefix}`;
  }
};

/**
 * If user is looking at an arctic source, and is going to the source homepage from the source history,
 * this converts the namespace from `/:branchName/*` to `/folder/* `, where `*` is any nested folders
 **/
export const convertCatalogToSourceSpace = (namespace: string) => {
  const modifiedNamespace = namespace.split("/");
  modifiedNamespace.shift();
  const sourceNamespace = namespace ? modifiedNamespace.join("/") : namespace;
  return sourceNamespace ? `/folder/${sourceNamespace}` : sourceNamespace;
};

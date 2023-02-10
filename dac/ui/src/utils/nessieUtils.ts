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
import { COMMIT_TYPE, NESSIE_REF_PREFIX } from "@app/constants/nessie";
import { NessieRootState, NessieState } from "@app/types/nessie";
import { isDefaultReferenceLoading } from "@app/selectors/nessie/nessie";
import { Branch } from "@app/services/nessie/client";
import { store } from "@app/store/store";
import apiUtils from "@app/utils/apiUtils/apiUtils";
import moize from "moize";
import { isVersionedSource } from "./sourceUtils";

export function getShortHash(hash?: string) {
  return hash && hash.length > 6 ? hash.substring(0, 6) : hash;
}

export function getIconByType(refType: string, hash?: string | null) {
  if (hash) return "vcs/commit";
  else if (refType === "TAG") return "vcs/tag";
  else return "vcs/branch";
}

export function getFullPathByType(refName: string, hash?: string | null) {
  let path = refName;
  if (hash) path = path + ` @ ${getShortHash(hash)}`;
  return path;
}

export function getTypeAndValue(state?: NessieState | null) {
  let value = null;
  if (!state) return null;
  if (state.hash) {
    value = {
      type: COMMIT_TYPE,
      value: state.hash,
    };
  } else if (
    state.reference &&
    ["BRANCH", "TAG"].includes(state.reference.type)
  ) {
    value = {
      type: state.reference.type,
      value: (state.reference as Branch).name,
    };
  }
  return value;
}

function getNessieState(nessie: NessieRootState, stateKey: string) {
  if (!nessie || !stateKey) return null;
  return nessie[stateKey]; //Send reference if it exists in Nessie State
}

//Ref parameter object that is sent to endpoints
export function getRefParams(nessie: NessieRootState, stateKey: string) {
  const state = getNessieState(nessie, stateKey);
  const value = getTypeAndValue(state);
  if (!value) {
    return {};
  } else {
    return { ref: value };
  }
}

//Ref parameter object that is sent to endpoints
export function getRefQueryParams(nessie: NessieRootState, stateKey: string) {
  const state = getNessieState(nessie, stateKey);
  const value = getTypeAndValue(state);
  if (!value) {
    return {};
  } else {
    return {
      refType: value.type,
      refValue: value.value,
    };
  }
}

export function getRefQueryParamsFromPath(
  fullPath: string | string[],
  nessie: NessieRootState,
  sep = "."
) {
  const [sourceName] =
    typeof fullPath === "string" ? fullPath.split(sep) : fullPath;
  return getRefQueryParams(nessie, sourceName.replace(/"/g, ""));
}

export function getRefQueryParamsFromDataset(fullPath: string[]) {
  const { nessie } = store.getState();
  return getRefQueryParamsFromPath(fullPath, nessie);
}

export function getNessieReferencePayload(nessie?: NessieRootState | null) {
  if (!nessie) return {};

  const stateKeys = Object.keys(nessie).filter(
    (key) => key && key.startsWith(NESSIE_REF_PREFIX)
  );
  return stateKeys.reduce((acc, key) => {
    const refState = nessie[key];
    const source = key.substring(NESSIE_REF_PREFIX.length, key.length);

    const value = getTypeAndValue(refState);
    if (value != null && source) acc[source] = value;

    return acc;
  }, {} as any);
}

export function getReferenceListForTransform(
  referencePayload = {} as { [key: string]: any } | null
) {
  if (referencePayload == null) return [];
  return Object.keys(referencePayload).map((key) => {
    const values = referencePayload[key];
    return {
      sourceName: key,
      reference: values,
    };
  });
}

export function getProjectUrl(id?: string) {
  //@ts-ignore
  return `${window.location.protocol}${apiUtils.getAPIVersion("NESSIE", {
    projectId: id,
  })}`;
}

export function getArcticProjectUrl(id?: string) {
  //@ts-ignore
  return `${window.location.protocol}${apiUtils.getAPIVersion("ARCTIC", {
    projectId: id,
  })}`;
}

export function getProjectIdFromUrl(url?: any) {
  if (!url || typeof url !== "string") return "";
  const value = url.substring(url.lastIndexOf("/") + 1, url.length) || "";
  return value.replace("/", "");
}

export function isBranchSelected(state?: NessieState) {
  if (!state) return false;

  const isLoading = isDefaultReferenceLoading(state);
  if (isLoading) return false; //Still loading, return false

  if (state.hash || state.reference?.type !== "BRANCH") return false;
  return state?.reference?.type === "BRANCH";
}

export const getSourceByName = moize(function (
  name: string,
  sources?: Array<{ name: string; type: string }>
) {
  return (sources || []).find(
    (cur) => cur.name === name && isVersionedSource(cur.type)
  );
});

export function parseNamespaceUrl(url: string, path: string) {
  if (url === "/") return undefined;
  return url.replace(`/${path}/`, "").split("/");
}

type CatalogSourceConfig = { arcticCatalogId: string };
type NessieSourceConfig = { nessieEndpoint: string };
function isArcticCatalogConfig(
  config: CatalogSourceConfig | NessieSourceConfig
): config is CatalogSourceConfig {
  return (config as CatalogSourceConfig).arcticCatalogId != null;
}

export function getEndpointFromSourceConfig(
  config?: CatalogSourceConfig | NessieSourceConfig
) {
  if (!config) return "";
  if (isArcticCatalogConfig(config)) {
    return getArcticProjectUrl(config.arcticCatalogId);
  } else {
    return config.nessieEndpoint;
  }
}

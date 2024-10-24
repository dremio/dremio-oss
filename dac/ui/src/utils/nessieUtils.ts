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
import { COMMIT_TYPE, NESSIE_REF_PREFIX } from "#oss/constants/nessie";
import { NessieRootState, NessieState } from "#oss/types/nessie";
import { isDefaultReferenceLoading } from "#oss/selectors/nessie/nessie";
import { Branch } from "#oss/services/nessie/client";
import { store } from "#oss/store/store";
import apiUtils from "#oss/utils/apiUtils/apiUtils";
import moize from "moize";
import { isVersionedSource } from "@inject/utils/sourceUtils";
import { isVersionedSoftwareSource } from "@inject/constants/sourceTypes";
import { NESSIE_PROXY_URL_V2 } from "#oss/constants/Api";
import { getDatasetByPath } from "./datasetTreeUtils";
import { PHYSICAL_DATASET, VIRTUAL_DATASET } from "#oss/constants/datasetTypes";
import Immutable from "immutable";
import type { Script } from "dremio-ui-common/sonar/scripts/Script.type.js";

export function getShortHash(hash?: string) {
  return hash && hash.length > 6 ? hash.substring(0, 8) : hash;
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
  sep = ".",
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
    (key) => key && key.startsWith(NESSIE_REF_PREFIX),
  );
  const keysAdded: string[] = [];
  return stateKeys.reduce((acc, key) => {
    const refState = nessie[key];
    const source = key.substring(NESSIE_REF_PREFIX.length, key.length);

    const value = getTypeAndValue(refState);
    // Case insensitive, ignore duplicate keys that have different casing
    if (value != null && source && !keysAdded.includes(source.toLowerCase())) {
      acc[source] = value;
      keysAdded.push(source.toLowerCase());
    }

    return acc;
  }, {} as any);
}

// Returns reference state for populated entries (has hash or reference name)
export function getStateRefsOmitted(
  nessie?: NessieRootState | null,
): NessieRootState {
  const result = {} as NessieRootState;
  if (!nessie) return result;

  const stateKeys = Object.keys(nessie).filter(
    (key) => key && !key.startsWith(NESSIE_REF_PREFIX),
  );
  return stateKeys.reduce((acc, cur) => {
    acc[cur] = nessie[cur];
    return acc;
  }, result);
}

export function getReferenceListForTransform(
  referencePayload = {} as { [key: string]: any } | null,
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

export function getReferencesListForScript(nessie?: NessieRootState) {
  const referencesObject = getNessieReferencePayload(nessie);
  return getReferenceListForTransform(referencesObject);
}

export function convertReferencesListToRootState(
  state: NessieRootState,
  referencesList: Script["referencesList"],
) {
  const newState = { ...state };

  referencesList.forEach((reference: any) => {
    if (reference.reference.type === "COMMIT") {
      newState[`${NESSIE_REF_PREFIX}${reference.sourceName}`] = {
        reference: {
          type: "BRANCH",
          name: "main",
        },
        defaultReference: null,
        hash: reference.reference.value,
        date: null,
        loading: {},
        errors: {},
      };
    } else {
      newState[`${NESSIE_REF_PREFIX}${reference.sourceName}`] = {
        reference: {
          type: reference.reference.type,
          name: reference.reference.value,
        },
        defaultReference: null,
        hash: null,
        date: null,
        loading: {},
        errors: {},
      };
    }
  });

  return newState;
}

export function getArcticProjectUrl(
  id: string | undefined,
  nessieVersion?: string,
) {
  //@ts-ignore
  return `${window.location.protocol}${apiUtils.getAPIVersion("ARCTIC", {
    projectId: id,
    nessieVersion,
  })}`;
}

export function isBranchSelected(state?: NessieState) {
  if (!state) return false;

  const isLoading = isDefaultReferenceLoading(state);
  if (isLoading) return false; //Still loading, return false

  if (state.hash || state.reference?.type !== "BRANCH") return false;
  return state?.reference?.type === "BRANCH";
}

export const getSourceByName = moize(
  (name: string, sources?: Immutable.List<any>) => {
    return (sources || Immutable.List()).find(
      (cur) => cur.get("name") === name && isVersionedSource(cur.get("type")),
    );
  },
);

export const getRawSourceByName = moize(
  (name: string, sources?: Immutable.List<any>) => {
    return (sources || Immutable.List()).find(
      (cur) => cur.get("name") === name,
    );
  },
);

export const getSourceById = (
  id: string,
  sources?: Immutable.Map<any, any>,
) => {
  const source = sources?.get(id);
  return isVersionedSource(source?.get("type")) ? source : null;
};

export function parseNamespaceUrl(url: string, path: string) {
  if (url === "/") return undefined;
  return url.replace(`/${path}/`, "").split("/");
}

type SourceType = {
  name: string;
  type: string;
  config?: CatalogSourceConfig | NessieSourceConfig;
};
type CatalogSourceConfig = { arcticCatalogId: string };
type NessieSourceConfig = { nessieEndpoint: string };
export function isArcticCatalogConfig(
  config?: CatalogSourceConfig | NessieSourceConfig,
): config is CatalogSourceConfig {
  return (config as CatalogSourceConfig)?.arcticCatalogId != null;
}

function getEndpointFromSourceConfig(
  config: CatalogSourceConfig | NessieSourceConfig | undefined,
  nessieVersion: string,
) {
  if (!config) return "";
  if (isArcticCatalogConfig(config)) {
    return getArcticProjectUrl(config.arcticCatalogId, nessieVersion);
  } else {
    return config.nessieEndpoint;
  }
}

function getNessieSourceUrl(sourceName: string) {
  return `${NESSIE_PROXY_URL_V2}/${`source/${encodeURIComponent(sourceName)}`}`;
}

export function getEndpointFromSource(
  source: SourceType | undefined,
  nessieVersion = "v2",
) {
  if (!source) return "";
  if (isVersionedSoftwareSource(source.type)) {
    return getNessieSourceUrl(source.name);
  }

  return getEndpointFromSourceConfig(source.config, nessieVersion);
}

export function getSqlATSyntax(sourceName?: string) {
  if (!sourceName) return "";

  const state = store.getState().nessie?.[sourceName];

  if (state?.hash) {
    return ` AT COMMIT "${state.hash}"`;
  } else if (state?.reference?.type === "TAG") {
    return ` AT TAG "${state.reference.name}"`;
  } else if (state?.reference?.name) {
    return ` AT BRANCH "${state.reference.name}"`;
  } else {
    return "";
  }
}

export function addDatasetATSyntax(pathList?: string[]) {
  if (!pathList?.length) return "";

  const dataset = getDatasetByPath(pathList);
  if (
    dataset &&
    (dataset.type === PHYSICAL_DATASET || dataset.type === VIRTUAL_DATASET)
  ) {
    return getSqlATSyntax(pathList[0]);
  } else {
    return "";
  }
}

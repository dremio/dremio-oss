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
import { Branch } from "@app/services/nessie/client";
import { DremioV2Api as V2BetaApi } from "@app/types/nessie";
import { store } from "@app/store/store";
import { NessieRootState, Reference } from "@app/types/nessie";
import type { Script } from "dremio-ui-common/sonar/scripts/Script.type.js";

export const INIT_REFS = "NESSIE_INIT_REFS";
type InitRefsAction = {
  type: typeof INIT_REFS;
};
export function initRefs() {
  return (dispatch: any) => dispatch({ type: INIT_REFS });
}
export type DatasetReference = {
  [key: string]: { type: string; value: string };
};
export const SET_REFS = "NESSIE_SET_REFS";
type SetRefsAction = {
  type: typeof SET_REFS;
  payload: DatasetReference;
};

export const RESET_REFS = "NESSIE_RESET_REFS";
type ResetRefsAction = {
  type: typeof RESET_REFS;
  payload: DatasetReference;
};

export const REMOVE_ENTRY = "NESSIE_REMOVE_ENTRY";
type RemoveEntryAction = {
  type: typeof REMOVE_ENTRY;
  payload: string;
};

export const SET_REFERENCES_LIST = "NESSIE_SET_REFERENCES_LIST";
type SetRefsList = {
  type: typeof SET_REFERENCES_LIST;
  payload: Script["referencesList"];
};

export function removeEntry(payload: RemoveEntryAction["payload"]) {
  return (dispatch: any) => dispatch({ type: REMOVE_ENTRY, payload });
}
export function setRefs(payload: NessieRootState) {
  return (dispatch: any) => dispatch({ type: SET_REFS, payload });
}

export function setRefsFromScript(payload: Script["referencesList"]) {
  return { type: SET_REFERENCES_LIST, payload };
}

//Resets all refs, initializes any provided refs
export function resetRefs(payload: DatasetReference) {
  return (dispatch: any) => dispatch({ type: RESET_REFS, payload });
}

export type NessieRootActionTypes =
  | InitRefsAction
  | SetRefsAction
  | ResetRefsAction
  | RemoveEntryAction
  | ResetNessieStateAction
  | SetRefsList;

type SourceType = { source: string; meta?: any };

export const SET_REF = "NESSIE_SET_REF";
export const SET_REF_REQUEST = "NESSIE_SET_REF_REQUEST";
export const SET_REF_REQUEST_FAILURE = "NESSIE_SET_REF_REQUEST_FAILURE";
export type SetReferenceAction = SourceType & {
  type: typeof SET_REF;
  payload: {
    reference: Reference | null;
    hash?: string | null;
    date?: Date | null;
  };
};
type SetReferenceFailureAction = {
  type: typeof SET_REF_REQUEST_FAILURE;
} & SourceType;
export function setReference(
  payload: SetReferenceAction["payload"],
  source: string,
): NessieActionTypes {
  return {
    type: SET_REF,
    payload,
    source,
    // Triggers reload of home contents when reference changes
    meta: { invalidateViewIds: ["HomeContents"] }, //TODO Importing this const was breaking imports from this file
  };
}
export const DEFAULT_REF_REQUEST = "NESSIE_DEFAULT_REF_REQUEST";
export const DEFAULT_REF_REQUEST_SUCCESS = "NESSIE_DEFAULT_REF_REQUEST_SUCCESS";
export const DEFAULT_REF_REQUEST_FAILURE = "NESSIE_DEFAULT_REF_REQUEST_FAILURE";
type FetchDefaultBranchAction = {
  type: typeof DEFAULT_REF_REQUEST;
} & SourceType;
type FetchDefaultBranchFailureAction = {
  type: typeof DEFAULT_REF_REQUEST_FAILURE;
} & SourceType;
type FetchDefaultBranchSuccessAction = {
  type: typeof DEFAULT_REF_REQUEST_SUCCESS;
  payload: Reference | null;
} & SourceType;

export const COMMIT_BEFORE_TIME_REQUEST = "NESSIE_COMMIT_BEFORE_TIME_REQUEST";
export const COMMIT_BEFORE_TIME_REQUEST_SUCCESS =
  "NESSIE_COMMIT_BEFORE_TIME_REQUEST_SUCCESS";
export const COMMIT_BEFORE_TIME_REQUEST_FAILURE =
  "NESSIE_COMMIT_BEFORE_TIME_REQUEST_FAILURE";
type FetchCommitBeforeTimeAction = {
  type: typeof COMMIT_BEFORE_TIME_REQUEST;
  payload: number;
} & SourceType;
type FetchCommitBeforeTimeSuccessAction = {
  type: typeof COMMIT_BEFORE_TIME_REQUEST_SUCCESS;
} & SourceType;
type FetchCommitBeforeTimeFailureAction = {
  type: typeof COMMIT_BEFORE_TIME_REQUEST_FAILURE;
} & SourceType;

export const NESSIE_RESET_STATE = "NESSIE_RESET_STATE";
type ResetNessieStateAction = {
  type: typeof NESSIE_RESET_STATE;
} & SourceType;

export type NessieActionTypes =
  | SetReferenceAction
  | SetReferenceFailureAction
  | FetchDefaultBranchAction
  | FetchDefaultBranchSuccessAction
  | FetchDefaultBranchFailureAction
  | FetchCommitBeforeTimeAction
  | FetchCommitBeforeTimeSuccessAction
  | FetchCommitBeforeTimeFailureAction
  | ResetNessieStateAction;

export function resetNessieState() {
  return (dispatch: any) => {
    return dispatch({
      type: NESSIE_RESET_STATE,
    });
  };
}

export function fetchDefaultReferenceIfNeeded(source: string, api: V2BetaApi) {
  return async (dispatch: any) => {
    const nessie = store.getState().nessie as NessieRootState;
    if (nessie[source]?.defaultReference) return;
    return dispatch(fetchDefaultReference(source, api));
  };
}

function fetchDefaultReference(source: string, api: V2BetaApi) {
  return async (dispatch: any) => {
    if (!source) return;
    dispatch({ type: DEFAULT_REF_REQUEST, source });
    try {
      const { reference } = await api.memoGetDefaultReference();
      dispatch({
        type: DEFAULT_REF_REQUEST_SUCCESS,
        payload: reference,
        source,
      });
    } catch (e: any) {
      dispatch({
        type: DEFAULT_REF_REQUEST_FAILURE,
        source,
        payload: "There was an error fetching the versioned entity.",
      });
    }
  };
}

export function fetchBranchReference(
  source: string,
  api: V2BetaApi,
  initialRef?: Branch,
) {
  return async (dispatch: any) => {
    dispatch({ type: SET_REF_REQUEST, source });
    try {
      if (!initialRef?.name) return;
      // https://github.com/projectnessie/nessie/issues/6210
      //@ts-ignore
      const { reference } = (await api.getReferenceByNameV2({
        ref: initialRef?.name,
      })) as Reference;

      dispatch({
        type: SET_REF,
        payload: {
          reference: {
            ...reference,
            hash: initialRef?.hash ?? null,
          },
          hash: initialRef?.hash ?? null,
        },
        source,
      });
    } catch (e) {
      dispatch({
        type: SET_REF_REQUEST_FAILURE,
        payload: `There was an error fetching the reference: ${initialRef?.name}.`,
        source,
        meta: {
          notification: {
            message: laDeprecated(
              `There was an error fetching the reference: ${initialRef?.name}.`,
            ),
            level: "error",
          },
        },
      });
    }
  };
}

export function fetchCommitBeforeTime(
  reference: Reference | null,
  date: Date,
  source: string,
  api: V2BetaApi,
) {
  return async (dispatch: any) => {
    let result = null;
    if (!reference) return result;
    dispatch({ type: COMMIT_BEFORE_TIME_REQUEST, source });
    try {
      const timestampISO = date.toISOString();
      const log = await api.getCommitLogV2({
        ref: reference.name,
        maxRecords: 1,
        filter: `timestamp(commit.commitTime) <= timestamp('${timestampISO}')`,
      });
      const hash = log?.logEntries?.[0]?.commitMeta?.hash || "";
      if (hash) {
        dispatch({ type: COMMIT_BEFORE_TIME_REQUEST_SUCCESS, source });
        result = { reference, hash, date };
      } else {
        dispatch({
          type: COMMIT_BEFORE_TIME_REQUEST_FAILURE,
          source,
          meta: {
            notification: {
              message: laDeprecated("No commit found for provided date."),
              level: "warning",
              autoDismiss: 3,
            },
          },
        });
      }
    } catch (e) {
      dispatch({
        type: COMMIT_BEFORE_TIME_REQUEST_FAILURE,
        source,
        meta: {
          notification: {
            message: laDeprecated("There was an error fetching the commit."),
            level: "error",
          },
        },
      });
    }

    return result;
  };
}

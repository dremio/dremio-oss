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
import { DefaultApi, Reference } from '@app/services/nessie/client';
import { oc } from 'ts-optchain';
import { push } from 'react-router-redux';

type SourceType = { source: string, meta?: any };

export const SET_REF = 'NESSIE_SET_REF';
type SetReferenceAction = SourceType & {
  type: typeof SET_REF,
  payload: {
      reference: Reference | null,
      hash?: string | null,
      date?: Date | null
  }
};
export function setReference(
  payload: SetReferenceAction['payload'],
  source: string
): NessieActionTypes {
  return {
    type: SET_REF,
    payload,
    source,
    // Triggers reload of home contents when reference changes
    meta: { invalidateViewIds: ['HomeContents'] } //TODO Importing this const was breaking imports from this file
  };
}
export const DEFAULT_REF_REQUEST = 'NESSIE_DEFAULT_REF_REQUEST';
export const DEFAULT_REF_REQUEST_SUCCESS = 'NESSIE_DEFAULT_REF_REQUEST_SUCCESS';
export const DEFAULT_REF_REQUEST_FAILURE = 'NESSIE_DEFAULT_REF_REQUEST_FAILURE';
type FetchDefaultBranchAction = { type: typeof DEFAULT_REF_REQUEST } & SourceType;
type FetchDefaultBranchFailureAction = { type: typeof DEFAULT_REF_REQUEST_FAILURE } & SourceType;
type FetchDefaultBranchSuccessAction = { type: typeof DEFAULT_REF_REQUEST_SUCCESS, payload: Reference | null } & SourceType;

export const COMMIT_BEFORE_TIME_REQUEST = 'NESSIE_COMMIT_BEFORE_TIME_REQUEST';
export const COMMIT_BEFORE_TIME_REQUEST_SUCCESS = 'NESSIE_COMMIT_BEFORE_TIME_REQUEST_SUCCESS';
export const COMMIT_BEFORE_TIME_REQUEST_FAILURE = 'NESSIE_COMMIT_BEFORE_TIME_REQUEST_FAILURE';
type FetchCommitBeforeTimeAction = { type: typeof COMMIT_BEFORE_TIME_REQUEST, payload: number } & SourceType;
type FetchCommitBeforeTimeSuccessAction = { type: typeof COMMIT_BEFORE_TIME_REQUEST_SUCCESS } & SourceType;
type FetchCommitBeforeTimeFailureAction = { type: typeof COMMIT_BEFORE_TIME_REQUEST_FAILURE } & SourceType;

export type NessieActionTypes = SetReferenceAction |
                                FetchDefaultBranchAction |
                                FetchDefaultBranchSuccessAction |
                                FetchDefaultBranchFailureAction |
                                FetchCommitBeforeTimeAction |
                                FetchCommitBeforeTimeSuccessAction |
                                FetchCommitBeforeTimeFailureAction;

export function fetchDefaultReference(source: string, api: DefaultApi) {
  return async (dispatch: any) => {
    dispatch({ type: DEFAULT_REF_REQUEST, source });
    try {
      const reference = await api.getDefaultBranch();
      dispatch({ type: DEFAULT_REF_REQUEST_SUCCESS, payload: reference, source });
    } catch (e) {
      dispatch({
        type: DEFAULT_REF_REQUEST_FAILURE,
        source,
        meta: {
          notification: {
            message: la('There was an error fetching the default branch.'),
            level: 'error'
          }
        }
      });
    }
  };
}
export function fetchCommitBeforeTime(reference: Reference | null, date: Date, source: string, api: DefaultApi, redirect = true) {
  return async (dispatch: any) => {
    if (!reference) return;
    dispatch({ type: COMMIT_BEFORE_TIME_REQUEST, source });
    try {
      const timestampISO = date.toISOString();
      const log = await api.getCommitLog({
        ref: reference.name,
        maxRecords: 1,
        filter: `timestamp(commit.commitTime) <= timestamp('${timestampISO}')`
      });
      const hash = oc(log.logEntries[0]).commitMeta.hash('');
      if (hash) {
        dispatch({ type: COMMIT_BEFORE_TIME_REQUEST_SUCCESS, source });
        dispatch({ type: SET_REF, payload: { reference, hash, date }, source });
        if (redirect) dispatch(push('/'));
      } else {
        dispatch({
          type: COMMIT_BEFORE_TIME_REQUEST_FAILURE,
          source,
          meta: {
            notification: {
              message: la('No commit found for provided date.'),
              level: 'warning',
              autoDismiss: 3
            }
          }
        });
      }
    } catch (e) {
      dispatch({
        type: COMMIT_BEFORE_TIME_REQUEST_FAILURE,
        source,
        meta: {
          notification: {
            message: la('There was an error fetching the commit.'),
            level: 'error'
          }
        }
      });
    }
  };
}

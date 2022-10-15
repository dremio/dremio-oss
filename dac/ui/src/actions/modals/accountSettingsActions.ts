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
export const SHOW_PAT_MODAL = "SHOW_PAT_MODAL";

export function showAddPatModal() {
  return (dispatch: any) => {
    return dispatch({
      type: SHOW_PAT_MODAL,
      payload: true,
    });
  };
}

export function hideAddPatModal() {
  return (dispatch: any) => {
    return dispatch({
      type: SHOW_PAT_MODAL,
      payload: false,
    });
  };
}

export const SHOW_ACCOUNT_SETTINGS_MODAL = "SHOW_ACCOUNT_SETTINGS_MODAL";

export function showAccountSettingsModal() {
  return (dispatch: any) => {
    return dispatch({
      type: SHOW_ACCOUNT_SETTINGS_MODAL,
      payload: true,
    });
  };
}

export function hideAccountSettingsModal() {
  return (dispatch: any) => {
    return dispatch({
      type: SHOW_ACCOUNT_SETTINGS_MODAL,
      payload: false,
    });
  };
}

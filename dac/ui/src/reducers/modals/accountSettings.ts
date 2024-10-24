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

const initialState = false;

export const showAddPatModal = (
  state = initialState,
  action: { type: string; payload: boolean },
) => {
  if (action.type === "SHOW_PAT_MODAL") {
    return action.payload;
  }
  return state;
};

export const showAccountSettingsModal = (
  state = initialState,
  action: { type: string; payload: boolean },
) => {
  if (action.type === "SHOW_ACCOUNT_SETTINGS_MODAL") {
    return action.payload;
  }
  return state;
};

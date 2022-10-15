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
import { DEFAULT_REF_REQUEST } from "@app/actions/nessie/nessie";
import { initialState } from "@app/reducers/nessie/nessie";
import { NessieRootState, NessieState } from "@app/types/nessie";
import { createLoadingSelector } from "../createLoadingSelector";

export const isDefaultReferenceLoading = createLoadingSelector([
  DEFAULT_REF_REQUEST,
]);

export function selectState(state: NessieRootState, source: string) {
  return (state[source] || initialState) as NessieState;
}

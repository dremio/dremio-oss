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

//@ts-ignore
import { Dremio } from "@dremio/dremio-js/community";

export type DremioContext = ReturnType<typeof Dremio>;

let dremioContext: (pid: string) => DremioContext;

export const setDremioContext = (ctx: () => DremioContext) =>
  (dremioContext = ctx);

export const getDremioContext = (pid: string): ReturnType<typeof Dremio> => {
  if (!dremioContext) {
    throw new Error("DremioContext is not configured");
  }
  return dremioContext(pid);
};

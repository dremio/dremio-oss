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

const temporaryTabPrefix = `__DREMIO_TMP__`;

export const addTemporaryPrefix = (name: string) => temporaryTabPrefix + name;

export const isTemporaryScriptName = (name: string) =>
  name.startsWith(temporaryTabPrefix);

export const isTemporaryScript = (script: any) => {
  if (!script || !script.name) {
    return false;
  }
  return isTemporaryScriptName(script.name);
};

export const stripTemporaryPrefix = (name: string) =>
  name.startsWith(temporaryTabPrefix)
    ? name.replace(temporaryTabPrefix, "")
    : name;

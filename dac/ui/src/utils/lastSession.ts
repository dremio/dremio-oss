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

const LAST_SESSION_KEY = "ls";

type LastSession = { username: string };

export const getLastSession = (): LastSession | null => {
  try {
    return JSON.parse(
      window.atob(globalThis.localStorage.getItem(LAST_SESSION_KEY)!),
    );
  } catch (e) {
    return null;
  }
};

export const setLastSession = (session: LastSession): void => {
  globalThis.localStorage.setItem(
    LAST_SESSION_KEY,
    window.btoa(JSON.stringify(session)),
  );
};

export const removeLastSession = (): void => {
  globalThis.localStorage.removeItem(LAST_SESSION_KEY);
};

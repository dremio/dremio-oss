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
import { installIntercom } from "./installIntercom";
import { bootIntercom } from "./bootIntercom";

const DEFAULT_OPTIONS = {};

/**
 * Install Intercom globally to the window
 */
export const initIntercom = (app_id: string, options = DEFAULT_OPTIONS) => {
  installIntercom(app_id, {
    ...DEFAULT_OPTIONS,
    ...options,
    app_id,
  });

  try {
    bootIntercom();
  } catch (e) {
    // Intercom failed to boot because there is no active user session
  }
};

// Make initIntercom available to the window so that it can be dynamically
// configured from GTM in the future
//@ts-ignore
globalThis.initIntercom = initIntercom;
declare global {
  interface Window {
    initIntercom: typeof initIntercom;
  }
}

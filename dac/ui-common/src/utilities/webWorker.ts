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

import { getLoggingContext } from "../contexts/LoggingContext";
import { consoleLogger } from "./consoleLogger";

// BEST PRACTICE TO IMPORT THIS FILE IN THE WEB WORKER ENTRYPOINT BEFORE ANY OTHER IMPORTS
// This is required in case of any other direct/transitive imports reference window in global scope

// Alias window to self (the global this for web workers) so any code that uses "window" will work (e.g. from files
// shared with the browser window app)
// Be careful though because things like localStorage are inaccessible to web workers
if (!self.window) {
  self.window = self;
}

export const additionalSetup = () => {
  getLoggingContext().registerHandler(consoleLogger);
};

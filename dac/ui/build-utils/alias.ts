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
import { resolve } from "path";

import { dynLoadPath } from "./dyn-load";

export const alias = {
  "@app": resolve(__dirname, "../src"),
  "dyn-load": dynLoadPath,
  "@ee": dynLoadPath,
  "@mui": resolve(__dirname, "../node_modules/@mui"),
  "@root": resolve(__dirname, "../"),
  react: resolve(__dirname, "../node_modules/react"),

  "leantable/react": resolve(
    __dirname,
    "../node_modules/leantable/dist-cjs/react",
  ),
};

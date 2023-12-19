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
import { alias } from "./alias";
import { InjectionResolver } from "../scripts/injectionResolver";

export const getResolve = ({
  additionalAliases = {},
}: {
  additionalAliases: Record<string, any>;
}) => ({
  extensions: [".js", ".jsx", ".ts", ".tsx", ".json"],
  modules: [
    resolve(__dirname, "../src"),
    "node_modules",
    resolve(__dirname, "../node_modules"), // TODO: this is ugly, needed to resolve module dependencies outside of src/ so they can find our main node_modules
  ],
  fallback: {
    path: require.resolve("path-browserify"),
    assert: require.resolve("assert"),
  },
  alias: {
    ...alias,
    ...additionalAliases,
  },
  plugins: [new InjectionResolver()],
});

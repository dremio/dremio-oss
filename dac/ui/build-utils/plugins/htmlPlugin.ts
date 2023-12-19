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
import HtmlWebpackPlugin from "html-webpack-plugin";
import { isProduction } from "../mode";
import { cssFilename, output } from "../output";
import { join, resolve } from "path";

const stripComments = process.env.DREMIO_STRIP_COMMENTS === "true";
const indexFilename = process.env.INDEX_FILENAME || "index.html";

const getDremioConfig = () => {
  return isProduction ? "JSON.parse('${dremio?js_string}')" : "null";
};

export const htmlPlugin = new HtmlWebpackPlugin({
  dremioConfig: getDremioConfig(),
  template: resolve(
    process.env.DREMIO_INJECTION_PATH ||
      process.env.DREMIO_DYN_LOADER_PATH ||
      join(__dirname, "../../src"),
    "index.html"
  ),
  filename: indexFilename,
  cache: false,
  files: {
    css: [cssFilename],
    js: [output.filename],
  },
  minify: {
    removeComments: stripComments,
  },
});

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
import { join } from "path";

export const cssFilename = "static/css/[name].[contenthash:8].css";

let outputPath = join(__dirname, "../build");

const pathArg = "--output-path=";
for (const param of process.argv) {
  if (param.toLowerCase().startsWith(pathArg)) {
    outputPath = param.substr(pathArg.length);
  }
}

export const output = {
  publicPath: "/",
  path: outputPath,
  filename: "static/js/[name].[contenthash:8].js",
  chunkFilename: "static/js/[name].[contenthash:8].chunk.js",
  sourceMapFilename: "sourcemaps/[file].map",
  assetModuleFilename: "static/media/[name].[hash][ext]",
};

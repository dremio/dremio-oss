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
//@ts-nocheck
import path from "path";
import { bundleAnalyzer } from "./build-utils/plugins/bundleAnalyzer";
import { optimization } from "./build-utils/optimization";
import { devServer } from "./build-utils/devServer";
import { output } from "./build-utils/output";
import { stats } from "./build-utils/stats";
import { getRules } from "./build-utils/rules";
import { getResolve } from "./build-utils/resolve";
import { sentryPlugin } from "./build-utils/plugins/sentry";
import { mode } from "./build-utils/mode";
import { devtool } from "./build-utils/devtool";
import { htmlPlugin } from "./build-utils/plugins/htmlPlugin";
import { bannerPlugin } from "./build-utils/plugins/bannerPlugin";
import { define } from "./build-utils/plugins/define";
import { css } from "./build-utils/plugins/css";
import { copy } from "./build-utils/plugins/copy";
import { tags } from "./build-utils/plugins/tags";
import webpack from "webpack";

const dcsPath = process.env.DREMIO_DCS_LOADER_PATH
  ? path.resolve(__dirname, process.env.DREMIO_DCS_LOADER_PATH)
  : null;

const config = {
  ignoreWarnings: [
    (warning) => {
      // Silence warnings about missing named exports in stubModule.js
      if (warning.message.indexOf("was not found in '") !== -1) {
        return true;
      }
    },
  ],
  devtool,
  optimization,
  devServer,
  output,
  stats,
  mode,
  entry: {
    app: [
      path.resolve(
        process.env.DREMIO_DCS_LOADER_PATH ||
        process.env.DREMIO_INJECTION_PATH ||
        process.env.DREMIO_DYN_LOADER_PATH ||
        path.join(__dirname, "src"),
        "index.tsx"
      ),
    ],
  },
  performance: {
    maxEntrypointSize: 10000000,
    maxAssetSize: 10000000,
  },
  module: {
    rules: getRules({ additionalIncludes: [...(dcsPath ? [dcsPath] : [])] }),
  },

  plugins: [
    bundleAnalyzer,
    css,
    bannerPlugin,
    htmlPlugin,
    tags,
    define,
    copy,
    sentryPlugin,
    new webpack.ProgressPlugin()
  ].filter(Boolean),

  resolve: getResolve({
    additionalAliases: {
      ...(dcsPath
        ? {
          "@dcs": dcsPath,
        }
        : {}),
    },
  }),
};

export default config;

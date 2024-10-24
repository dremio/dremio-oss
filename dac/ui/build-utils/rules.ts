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
import { injectionPath } from "../scripts/injectionResolver";
import { dynLoadPath } from "./dyn-load";
//@ts-ignore
import MiniCssExtractPlugin from "mini-css-extract-plugin";

const injectionPathAsList = injectionPath ? [injectionPath] : [];

const babelOptions = {
  configFile: resolve(__dirname, "../.babelrc.js"),
};

const babelLoader = {
  loader: "babel-loader",
  options: {
    ...babelOptions,
    cacheDirectory: true,
  },
};

const getStyleLoader = (isCss: boolean) => {
  const otherLoaders = [!isCss && "less-loader"].filter(Boolean);
  return {
    test: isCss ? /\.css$/ : /\.less$/,
    use: [
      {
        loader: MiniCssExtractPlugin.loader,
        options: {
          // Breaking change that's incompatible until we apply these adjustments,
          // see https://github.com/webpack-contrib/mini-css-extract-plugin/releases/tag/v1.0.0
          esModule: false,
        },
      },
      {
        loader: "css-loader",
        options: {
          // Breaking change that's incompatible until we apply these adjustments,
          // see https://github.com/webpack-contrib/css-loader/releases/tag/v6.0.0
          esModule: false,
          modules: !isCss && {
            mode: "local",
            localIdentName: "[name]__[local]___[hash:base64:5]",
            exportLocalsConvention: "camelCase",
          },
          importLoaders: otherLoaders.length,
        },
      },
      ...otherLoaders,
    ],
  };
};

export const getRules = ({
  additionalIncludes = [],
}: {
  additionalIncludes: string[];
}) => [
  {
    test: /(SQLParsingWorker|LayoutWorker)\.worker\.[jt]s$/,
    use: { loader: "worker-loader" },
  },
  {
    test: /\.(js(x)?|ts(x)?)$/,
    exclude:
      /node_modules(?!\/regenerator-runtime|\/redux-saga|\/whatwg-fetch)/,
    include: [
      resolve(__dirname, "../"),
      dynLoadPath,
      ...injectionPathAsList,
      ...additionalIncludes,
    ],
    use: [babelLoader],
  },
  getStyleLoader(false),
  getStyleLoader(true),
  {
    test: /\.pattern$/,
    use: {
      loader: "glob-loader",
    },
  },
  {
    test: /\.png$/,
    type: "asset/resource",
  },
  {
    test: /(components|pages)\/.*\.svg(\?.*)?$/,
    type: "asset/resource",
  },
  {
    test: /(ui-lib)\/.*\.svg(\?.*)?$/,
    type: "asset/resource",
  },
  {
    test: /\.(woff(2)?|ttf|eot|gif|lottie|svg)(\?.*)?$/,
    type: "asset/resource",
  },
  {
    test: /\.ya?ml$/,
    use: "yaml-loader",
  },
  {
    test: /\.s[ac]ss$/i,
    use: [
      {
        loader: MiniCssExtractPlugin.loader,
      },
      "css-loader",
      "sass-loader",
    ],
  },
];

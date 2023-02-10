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
const path = require("path");
const webpack = require("webpack");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CopyWebpackPlugin = require("copy-webpack-plugin");
const HtmlWebpackTagsPlugin = require("html-webpack-tags-plugin");
const SentryCliPlugin = require("@sentry/webpack-plugin");
const MiniCssExtractPlugin = require("mini-css-extract-plugin");
const CssMinimizerPlugin = require("css-minimizer-webpack-plugin");
const TerserPlugin = require("terser-webpack-plugin");
const { getVersion, getEdition } = require("./scripts/versionUtils");
const dynLoader = require("./dynLoader");
const {
  injectionPath,
  InjectionResolver,
} = require("./scripts/injectionResolver");
const BundleAnalyzerPlugin =
  require("webpack-bundle-analyzer").BundleAnalyzerPlugin;

dynLoader.applyNodeModulesResolver();

const enableBundleAnalyzer = process.env.ENABLE_BUNDLE_ANALYZER === "true";
const isProductionBuild = process.env.NODE_ENV === "production";
const isBeta = process.env.DREMIO_BETA === "true";
const skipSourceMapUpload = process.env.SKIP_SENTRY_STEP === "true";
const dremioVersion = getVersion();
const devtool = isProductionBuild ? "hidden-source-map" : "eval-source-map"; // chris says: '#cheap-eval-source-map' is really great for debugging
const isStaticDremioConfig = process.env.STATIC_DREMIO_CONFIG === "true";
const stripComments = process.env.DREMIO_STRIP_COMMENTS === "true";

const dcsPath = process.env.DREMIO_DCS_LOADER_PATH
  ? path.resolve(__dirname, process.env.DREMIO_DCS_LOADER_PATH)
  : null;

const indexFilename = process.env.INDEX_FILENAME || "index.html";

let outputPath = path.join(__dirname, "build");
const pathArg = "--output-path=";
//output path may be overwritten by passing a command line argument
for (const param of process.argv) {
  if (param.toLowerCase().startsWith(pathArg)) {
    outputPath = param.substr(pathArg.length);
  }
}

const babelOptions = {
  configFile: path.resolve(__dirname, ".babelrc.js"),
};

const getDremioConfig = () => {
  let config = isProductionBuild ? "JSON.parse('${dremio?js_string}')" : "null";

  if (isProductionBuild && isStaticDremioConfig) {
    config = JSON.stringify({ edition: getEdition() });
  }
  return config;
};

const babelLoader = {
  loader: "babel-loader",
  options: {
    ...babelOptions,
    cacheDirectory: true,
  },
};

const getStyleLoader = (isCss) => {
  const otherLoaders = [!isCss && "less-loader"].filter(Boolean);
  return {
    test: isCss ? /\.css$/ : /\.less$/,
    use: [
      {
        loader: MiniCssExtractPlugin.loader,
      },
      {
        loader: "css-loader",
        options: {
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

const injectionPathAsList = injectionPath ? [injectionPath] : [];
const dcsPathAsList = dcsPath ? [dcsPath] : [];

const rules = [
  {
    test: /\.(js(x)?|ts(x)?)$/,
    exclude:
      /node_modules(?!\/regenerator-runtime|\/redux-saga|\/whatwg-fetch)/,
    include: [
      __dirname,
      dynLoader.path,
      ...injectionPathAsList,
      ...dcsPathAsList,
    ],
    use: [babelLoader],
  },
  {
    test: /\.worker\.js$/,
    use: { loader: "worker-loader" },
  },
  getStyleLoader(false),
  getStyleLoader(true),
  {
    test: /art\/.*\.svg$/,
    use: [
      babelLoader,
      {
        loader: "react-svg-loader",
        options: {
          svgo: {
            plugins: [{ removeDimensions: true }, { convertPathData: false }], // disable convertPathData pending https://github.com/svg/svgo/issues/863
          },
        },
      },
    ],
  },
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
    // for legacy
    test: /(components|pages)\/.*\.svg(\?.*)?$/,
    type: "asset/resource",
  },
  {
    test: /(ui-lib)\/.*\.svg(\?.*)?$/,
    type: "asset/resource",
  },
  {
    test: /\.(woff(2)?|ttf|eot|gif)(\?.*)?$/,
    type: "asset/resource",
  },
];

const outFilenameJsTemplate = "static/js/[name].[contenthash:8].js";
const outFilenameJsChunkTemplate = "static/js/[name].[contenthash:8].chunk.js";
const outFilenameCssTemplate = "static/css/[name].[contenthash:8].css";

const config = {
  // abort process on errors
  bail: true,
  mode: isProductionBuild ? "production" : "development",
  entry: {
    app: [path.resolve(__dirname, "src/index.js")],
  },
  output: {
    publicPath: "/",
    path: outputPath,
    filename: outFilenameJsTemplate,
    chunkFilename: outFilenameJsChunkTemplate,
    sourceMapFilename: "sourcemaps/[file].map",
    assetModuleFilename: "static/media/[name].[hash][ext]",
  },
  stats: {
    assets: false,
    children: false,
    chunks: false,
    entrypoints: true,
    logging: "warn",
    modules: false,
  },
  performance: {
    maxEntrypointSize: 10000000,
    maxAssetSize: 10000000,
  },
  module: {
    rules,
  },
  devtool,
  plugins: [
    new webpack.ProvidePlugin({
      process: "process/browser",
    }),
    enableBundleAnalyzer && new BundleAnalyzerPlugin(),
    new MiniCssExtractPlugin({
      // Options similar to the same options in webpackOptions.output
      // all options are optional
      filename: outFilenameCssTemplate,
      chunkFilename: outFilenameCssTemplate,
      ignoreOrder: false, // Enable to remove warnings about conflicting order
    }),
    new webpack.BannerPlugin(require(dynLoader.path + "/webpackBanner")),
    new HtmlWebpackPlugin({
      dremioConfig: getDremioConfig(),
      template: "./src/index.html",
      filename: indexFilename,
      cache: false, // make sure rebuilds kick BuildInfo too
      files: {
        css: [outFilenameCssTemplate],
        js: [outFilenameJsTemplate],
      },
      minify: {
        removeComments: stripComments,
      },
    }),
    new HtmlWebpackTagsPlugin({
      tags: ["static/js/jsPlumb-2.1.4-min.js"],
    }),
    // 'process.env.NODE_ENV' does not work, despite the fact that it is a recommended way, according
    // to documentation (see https://webpack.js.org/plugins/define-plugin/)
    new webpack.DefinePlugin({
      // todo
      // copy some variables that are required by UI code
      "process.env": [
        "DREMIO_RELEASE",
        "DREMIO_VERSION",
        "EDITION_TYPE",
        "SKIP_SENTRY_STEP",
        "DCS_V2_URL",
        "DCS_V3_URL",
        "DREMIO_BETA",
        "ENABLE_MSW",
      ].reduce(
        (resultObj, variableToCopy) => {
          resultObj[variableToCopy] = JSON.stringify(
            process.env[variableToCopy]
          );
          return resultObj;
        },
        {
          // This is for React: https://facebook.github.io/react/docs/optimizing-performance.html#use-the-production-build
          // and some other utility methods
          // You probably want `utils/config` instead.
          NODE_ENV: JSON.stringify(
            isProductionBuild ? "production" : "development"
          ),
        }
      ),
    }),
    new CopyWebpackPlugin({
      patterns: [
        {
          from: `node_modules/monaco-editor/${
            isProductionBuild ? "min" : "dev"
          }/vs`,
          to: "vs",
        },
        {
          from: "public",
        },
        getEdition() === "ee" && {
          from: path.join(process.env.DREMIO_DYN_LOADER_PATH, "../public"),
        },
        {
          from: "node_modules/jsplumb/dist/js/jsPlumb-2.1.4-min.js",
          to: "static/js",
        },
        {
          from: `node_modules/dremio-ui-lib/icons`,
          to: "static/icons",
        },
        {
          from: `node_modules/dremio-ui-lib/images`,
          to: "static/images",
        },
        process.env.ENABLE_MSW === "true" && {
          from: "src/mockServiceWorker.js",
          to: "mockServiceWorker.js",
        },
      ].filter(Boolean),
    }),
    !skipSourceMapUpload &&
      new SentryCliPlugin({
        release: dremioVersion,
        include: outputPath,
        ignore: [
          "vs", // ignore monaco editor sources
          "**/*.css.map",
        ],
        configFile: path.resolve(__dirname, ".sentryclirc"),
        rewrite: true,
      }),
  ].filter(Boolean),
  optimization: {
    moduleIds: "deterministic",
    runtimeChunk: "single",
    splitChunks: {
      cacheGroups: {
        vendor: {
          test: /node_modules/,
          chunks: "initial",
          name: "vendor",
          enforce: true,
        },
      },
    },
    minimizer: [new TerserPlugin(), new CssMinimizerPlugin()],
  },
  resolve: {
    extensions: [".js", ".jsx", ".ts", ".tsx", ".json"],
    modules: [
      path.resolve(__dirname, "src"),
      "node_modules",
      path.resolve(__dirname, "node_modules"), // TODO: this is ugly, needed to resolve module dependencies outside of src/ so they can find our main node_modules
    ],
    // Webpack v4 previously supplied all of these Node polyfills, now we need to include
    // them manually.
    fallback: {
      path: require.resolve("path-browserify"),
      assert: require.resolve("assert"),
    },
    alias: {
      ...(dcsPath
        ? {
            "@dcs": dcsPath,
          }
        : {}),
      "dyn-load": dynLoader.path, // ref for std code to ref dynamic componentsd
      "@app": path.resolve(__dirname, "src"),
      "@root": path.resolve(__dirname),
      "Narwhal-Logo-With-Name-Light": path.resolve(
        isBeta
          ? "./src/components/Icon/icons/Narwhal-Logo-With-Name-Light-Beta.svg"
          : "./src/components/Icon/icons/Narwhal-Logo-With-Name-Light.svg"
      ),
      // Todo: Below lines are to fix the issue with 2 instances of react because of lib. Find a better fix for this. https://github.com/facebook/react/issues/13991
      react: path.resolve(__dirname, "node_modules/react"),
      "@mui": path.resolve(__dirname, "node_modules/@mui"),
    },
    plugins: [new InjectionResolver()],
  },
};

module.exports = config;

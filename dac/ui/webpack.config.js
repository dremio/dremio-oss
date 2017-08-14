/*
 * Copyright (C) 2017 Dremio Corporation
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
/*eslint no-sync: 0*/

const path = require('path');
const fs = require('fs');
const childProcess = require('child_process');
const webpack = require('webpack');
const autoprefixer = require('autoprefixer');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const ExtractTextPlugin = require('extract-text-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');
// todo: when webpack will be updated to v2 this plugin should be removed
const failPlugin = require('webpack-fail-plugin');
const userConfig = require('./webpackUtils/userConfig');
const dynLoader = require('./dynLoader');

dynLoader.applyNodeModulesResolver();

const isProductionBuild = process.env.NODE_ENV === 'production';
const minify = process.env.DREMIO_MINIFY === 'true';

const isBeta = process.env.DREMIO_BETA === 'true';

console.info(process.argv);
console.info({
  minify,
  isProductionBuild,
  isBeta,
  dynLoaderPath: dynLoader.path
});
console.info(userConfig);


const loaders = [
  { test: /\.gif$/, loader: 'url?limit=10000&mimetype=image/gif' },
  { test: /\.jpg$/, loader: 'url?limit=10000&mimetype=image/jpg' },
  { test: /\.png$/, loader: 'url?limit=10000&mimetype=image/png' },
  { test: /\.svg(\?v=[0-9]\.[0-9]\.[0-9])?$/, loader: 'url?limit=10000&mimetype=image/svg+xml' },
  { test: /\.(woff(2)?|ttf|eot)?(\?v=[0-9]\.[0-9]\.[0-9])?$/, loader: 'url?limit=1' },
  { test: /\.json$/, exclude: /node_modules|forkedModules|vendor/, loader: 'json-loader'},
  { test: /\.yml$/, loader: 'yaml-parser', exclude: /node_modules|forkedModules|vendor/, include: __dirname},
  {
    test: /\.js$/,
    loader: 'babel',
    exclude: /node_modules(?!\/regenerator-runtime|\/redux-saga|\/whatwg-fetch)/,
    include:  [__dirname, dynLoader.path],
    query: Object.assign({ // eslint-disable-line no-restricted-properties
      'cacheDirectory': true
    }, JSON.parse(fs.readFileSync('.babelrc', 'utf8')))
  },
  !userConfig.enableEslintLoader ? [] : {
    test: /\.js$/,
    exclude: /node_modules|bower_components|coverage|forkedModules|vendor/,
    loader: "eslint-loader?{parser: 'babel-eslint'}",
    include: __dirname
  },
  {
    test: /\.css$/,
    loader: ExtractTextPlugin.extract('style-loader', 'css-loader!postcss-loader')
  },
  {
    test: /\.less$/,
    loader: ExtractTextPlugin.extract('style-loader', 'css-loader!postcss-loader!less-loader')
  }
];

class BuildInfo {
  apply(compiler) {
    compiler.plugin('compilation', function(compilation) {
      compilation.plugin('html-webpack-plugin-before-html-generation', function(htmlPluginData, callback) {
        let commit = childProcess.execSync('git rev-parse HEAD').toString().trim();
        let changes = childProcess.execSync('git status --porcelain --untracked-files=no').toString();
        changes = changes.replace(/\n?.*pom.xml$/gm, '').trim(); // ignore pom changes from builder
        if (changes) {
          commit += '\nWith uncommitted changes:\n' + changes;
        }

        // because config is relying on freemarker template variables to be interpreted by the server
        // at runtime, config has to be string (and not an object) otherwise, shouldEnableRSOD could
        // not be a boolean for example
        const config = `{
          serverEnvironment: ${JSON.stringify(isProductionBuild ? '${dremio.environment}' : null)},
          serverStatus: ${JSON.stringify(isProductionBuild ? '${dremio.status}' : 'OK')},
          environment: ${JSON.stringify(isProductionBuild ? 'PRODUCTION' : 'DEVELOPMENT')},
          commit: ${JSON.stringify(commit)},
          ts: "${new Date()}",
          language: ${JSON.stringify(process.env.language)},
          useRunTimeLanguage: ${JSON.stringify(process.env.useRunTimeLanguage)},
          intercomAppId: ${JSON.stringify(isProductionBuild ? '${dremio.config.intercom.appid}' : userConfig.intercomAppId)},
          shouldEnableBugFiling: ${!isProductionBuild || '${dremio.debug.bug.filing.enabled?c}'},
          shouldEnableRSOD: ${!isProductionBuild || '${dremio.debug.rsod.enabled?c}'},
          supportEmailTo: ${JSON.stringify(isProductionBuild ? '${dremio.settings.supportEmailTo}' : 'noreply@dremio.com')},
          supportEmailSubjectForJobs: ${JSON.stringify(isProductionBuild ? '${dremio.settings.supportEmailSubjectForJobs}' : '')},
          outsideCommunicationDisabled: ${isProductionBuild ? '${dremio.settings.outsideCommunicationDisabled?c}' : false}
        }`;

        htmlPluginData.plugin.options.config = config;
        callback(null, htmlPluginData);
      });
    });
  }
}

const plugins = [
  new webpack.BannerPlugin(require(dynLoader.path + '/webpackBanner')),
  new webpack.HotModuleReplacementPlugin(),
  new webpack.NoErrorsPlugin(),
  new webpack.optimize.DedupePlugin(),
  new webpack.PrefetchPlugin('fixed-data-table-2'),
  new webpack.PrefetchPlugin('react-bootstrap'),
  new webpack.PrefetchPlugin('redux-devtools'),
  new webpack.PrefetchPlugin('react'),
  new webpack.PrefetchPlugin('redux-devtools-log-monitor'),
  new webpack.PrefetchPlugin('react-json-tree'),
  new webpack.PrefetchPlugin('react-date-range'),
  new webpack.PrefetchPlugin('moment'),
  new webpack.PrefetchPlugin('jquery'),
  new webpack.PrefetchPlugin('jsplumb/dist/js/jsPlumb-2.1.4-min.js'),
  new webpack.PrefetchPlugin('immutable'),
  new webpack.optimize.CommonsChunkPlugin('vendor', isProductionBuild ? 'vendor.[hash].js' : 'vendor.js'),
  new ExtractTextPlugin(isProductionBuild ? 'style.[contentHash].css' : 'style.css', {
    allChunks: true
  }),
  new HtmlWebpackPlugin({
    template: './src/index.html',
    cache: false // make sure rebuilds kick BuildInfo too
  }),
  new BuildInfo(),
  new webpack.DefinePlugin({
    // This is for React: https://facebook.github.io/react/docs/optimizing-performance.html#use-the-production-build
    // You probably want `window.config` instead.
    'process.env': { NODE_ENV: JSON.stringify(isProductionBuild ? 'production' : 'development') }
  }),
  new CopyWebpackPlugin([
    { from: 'src/favicon/favicons' }
  ]),
  failPlugin
];

if (minify) {
  plugins.push(
    new webpack.optimize.UglifyJsPlugin()
  );
}

const languageLoader = {
  test: /\.js$/,
  loaders: ['language-parser'],
  exclude: /node_modules|forkedModules|vendor/,
  include: __dirname
};

if (process.env.language) {
  loaders.push(languageLoader);
}

const polyfill = [
  './src/polyfills',
  'babel-polyfill'
];

// for hot reloading we require one chunk
const vendor = isProductionBuild
  ? [
    ...polyfill,
    'codemirror',
    'fixed-data-table-2',
    'immutable',
    'jquery',
    'lodash',
    'moment',
    'radium',
    'react',
    'react-bootstrap',
    'react-date-range',
    'react-dnd-html5-backend',
    'react-json-tree',
    'react-overlays',
    'react-redux',
    'react-router-redux',
    'react-router'
  ]
  : [];

const entry = {
  app: [
    'element-closest',
    ...!isProductionBuild ? polyfill : [],
    './src/regeneratorRuntimeDefault',
    './src/index'
  ],
  vendor
};

if (!isProductionBuild) {
  entry.app.push('webpack-hot-middleware/client');
  entry.app.push('./src/debug');
}

const config = {
  devtool: isProductionBuild ? 'source-map' : userConfig.sourceMaps, // chris says: '#cheap-eval-source-map' is really great for debugging
  entry,
  output: {
    path: path.join(__dirname, 'build'),
    filename: isProductionBuild ? 'bundle.[hash].js' : 'bundle.js',
    sourceMapFilename: isProductionBuild ? 'bundle.[hash].js.map' : 'bundle.js.map',
    publicPath: '/'
  },
  plugins,
  module: {
    noParse: '/node_modules|forkedModules|vendor/',
    loaders
  },
  resolveLoader: {
    root: path.resolve(__dirname, 'node_modules'), // required to resolve loaders in code outside tree
    alias: {
      'language-parser': path.join(__dirname, './webpackUtils/languageParser.js'),
      'yaml-parser': path.join(__dirname, './webpackUtils/yamlLoader.js')
    }
  },
  resolve: {
    root: [
      path.resolve(__dirname, 'src'),
      path.resolve(__dirname, 'node_modules')
    ],
    alias: {
      'dyn-load': dynLoader.path, // ref for std code to ref dynamic components
      react: path.resolve('./node_modules/react'),
      React: path.resolve('./node_modules/react'),
      immutable: path.resolve('./node_modules/immutable/dist/immutable.min.js'),
      jquery: path.resolve('./node_modules/jquery/dist/jquery.min.js'),
      d3: path.resolve('./node_modules/d3/d3.min.js'),
      'Narwhal-Logo-With-Name-Light': path.resolve(
        isBeta
          ? './src/components/Icon/icons/Narwhal-Logo-With-Name-Light-Beta.svg'
          : './src/components/Icon/icons/Narwhal-Logo-With-Name-Light.svg'
        )
    }
  },
  postcss() {
    return [ autoprefixer ];
  },
  eslint: {
    configFile: '.eslintrc'
  },
  stats: {
    children: true
  }
};

module.exports = config;

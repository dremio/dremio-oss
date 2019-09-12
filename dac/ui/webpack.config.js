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
const fs = require('fs');
const path = require('path');
const webpack = require('webpack');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const ExtractTextPlugin = require('extract-text-webpack-plugin');
const UglifyJSPlugin = require('uglifyjs-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');
const SentryCliPlugin = require('@sentry/webpack-plugin');

const { getVersion } = require('./scripts/versionUtils');
const dynLoader = require('./dynLoader');
dynLoader.applyNodeModulesResolver();

const isProductionBuild = process.env.NODE_ENV === 'production';
const minify = process.env.DREMIO_MINIFY === 'true';
const isBeta = process.env.DREMIO_BETA === 'true';
const isRelease = process.env.DREMIO_RELEASE === 'true';
const sentryAuthToken = process.env.SENTRY_AUTH_TOKEN;
const skipSourceMapUpload = process.env.SKIP_SENTRY_STEP === 'true';
const dremioVersion = getVersion();
const devtool = isProductionBuild ? 'source-map' : 'eval-source-map';  // chris says: '#cheap-eval-source-map' is really great for debugging


let outputPath = path.join(__dirname, 'build');
const pathArg = '--output-path=';
//output path may be overwritten by passing a command line argument
for (const param of process.argv) {
  if (param.toLowerCase().startsWith(pathArg)) {
    outputPath = param.substr(pathArg.length);
  }
}

console.info({
  dremioVersion,
  minify,
  isProductionBuild,
  isBeta,
  isRelease,
  dynLoaderPath: dynLoader.path,
  devtool,
  sentry: {
    skip: skipSourceMapUpload,
    token: sentryAuthToken
  },
  outputPath
});

const cssFileNameTemplate = '[name].[chunkhash].css';
const extractStyles = new ExtractTextPlugin({
  filename: cssFileNameTemplate,
  allChunks: true, // to load css for dynamically imported modules
  ignoreOrder: isProductionBuild // see https://github.com/redbadger/website-honestly/issues/128
});

const getLessLoader = isModules => {
  const rule = {
    test: /\.less$/
  };
  const otherLoaders = [
    { loader: 'postcss-loader', options: { config: { path: __dirname} } },
    { loader: 'less-loader' }
  ];
  const cssLoader = {
    loader: 'css-loader',
    options: {
      importLoaders: otherLoaders.length
    }
  };

  if (isModules) {
    cssLoader.options = {
      ...cssLoader.options,
      modules: true,
      camelCase: true,
      localIdentName: '[name]__[local]___[hash:base64:5]'
    };
  }

  rule.use = extractStyles.extract({
    use: [cssLoader, ...otherLoaders]
  });

  return rule;
};

class BuildInfo {
  apply(compiler) {
    compiler.plugin('compilation', function(compilation) {
      compilation.plugin('html-webpack-plugin-before-html-generation', function(htmlPluginData, callback) {
        // because config is relying on freemarker template variables to be interpreted by the server
        // at runtime, config has to be string (and not an object). ${dremio} is a freemaker variable.
        const config = isProductionBuild ? 'JSON.parse(\'${dremio?js_string}\')' : 'null';

        htmlPluginData.plugin.options.config = config;
        callback(null, htmlPluginData);
      });
    });
  }
}

const babelLoader = {
  loader: 'babel-loader',
  options: {
    // eslint-disable-next-line no-sync
    ...JSON.parse(fs.readFileSync(path.resolve(__dirname, '.babelrc'), 'utf8')),
    cacheDirectory: true
  }
};

const loaders = [
  {
    test: /art\/.*\.svg$/,
    use: [
      babelLoader,
      {
        loader: 'react-svg-loader',
        options: {
          svgo: {
            plugins: [{removeDimensions: true}, {convertPathData: false}] // disable convertPathData pending https://github.com/svg/svgo/issues/863
          }
        }
      }
    ]
  },
  {
    test : /\.js$/,
    exclude: /node_modules(?!\/regenerator-runtime|\/redux-saga|\/whatwg-fetch)/,
    include:  [__dirname, dynLoader.path],
    use: [babelLoader]
  },
  {
    test: /\.css$/,
    use: extractStyles.extract({
      use: [
        { loader: 'css-loader', options: { importLoaders: 1 } },
        { loader: 'postcss-loader', options: { config: { path: __dirname} } }
      ]
    })
  },
  {
    // oneOf is an interim solution to migrate to css modules
    oneOf: [
      getLessLoader(true)]
  },
  {
    test: /\.gif$/,
    use: {
      loader: 'url-loader',
      options: {
        limit: 10000,
        mimetype: 'image/gif'
      }
    }
  },
  {
    test: /\.pattern$/,
    use: {
      loader: 'glob-loader'
    }
  },
  {
    test: /\.jpg$/,
    use: {
      loader: 'url-loader',
      options: {
        limit: 10000,
        mimetype: 'image/jpg'
      }
    }
  },
  {
    test: /\.png$/,
    use: {
      loader: 'url-loader',
      options: {
        limit: 10000,
        mimetype: 'image/png'
      }
    }
  },
  {
    // for font-awesome and legacy
    test: /(font-awesome|components|pages)\/.*\.svg(\?.*)?$/,
    use: {
      loader: 'url-loader',
      options: {
        limit: 10000,
        mimetype: 'image/svg+xml'
      }
    }
  },
  {
    test: /\.(woff(2)?|ttf|eot)(\?.*)?$/,
    use: {
      loader: 'url-loader',
      options: {
        limit: 1
      }
    }
  }
];

const polyfill = [
  './src/polyfills',
  'isomorphic-fetch',
  'element-closest',
  'babel-polyfill',
  'url-search-params-polyfill',
  'abortcontroller-polyfill'
];

const outFileNameTemplate = '[name].[chunkhash].js';
const config = {
  // abort process on errors
  bail: true,
  entry: {
    app: [
      path.resolve(__dirname, 'src/index.js')
    ],
    vendor: [
      ...polyfill,
      'codemirror',
      'fixed-data-table-2',
      'immutable',
      'jquery',
      'lodash',
      'moment',
      'radium',
      'react',
      'react-date-range',
      'react-dnd-html5-backend',
      'react-json-tree',
      'react-overlays',
      'react-redux',
      'react-router-redux',
      'react-router'
    ]
  },
  output: {
    publicPath: '/',
    path: outputPath,
    filename: outFileNameTemplate,
    chunkFilename: outFileNameTemplate,
    sourceMapFilename: 'sourcemaps/[file].map'
  },
  module: {
    loaders
  },
  devtool,
  plugins: [
    new webpack.BannerPlugin(require(dynLoader.path + '/webpackBanner')),
    new webpack.HashedModuleIdsPlugin(),
    extractStyles,
    new webpack.optimize.CommonsChunkPlugin({
      name: 'vendor',
      filename: 'vendor.[chunkhash].js'
    }),
    new webpack.optimize.CommonsChunkPlugin({
      name:'manifest',
      minChunks: Infinity
    }),
    new HtmlWebpackPlugin({
      template: './src/index.html',
      cache: false, // make sure rebuilds kick BuildInfo too
      files: {
        css: [cssFileNameTemplate],
        js: [outFileNameTemplate]
      }
    }),
    new BuildInfo(),
    // 'process.env.NODE_ENV' does not work, despite the fact that it is a recommended way, according
    // to documentation (see https://webpack.js.org/plugins/define-plugin/)
    new webpack.DefinePlugin({
      // copy some variables that are required by UI code
      'process.env': ['DREMIO_RELEASE', 'DREMIO_VERSION', 'EDITION_TYPE', 'SKIP_SENTRY_STEP'].reduce((resultObj, variableToCopy) => {
        resultObj[variableToCopy] = JSON.stringify(process.env[variableToCopy]);
        return resultObj;
      }, {
        // This is for React: https://facebook.github.io/react/docs/optimizing-performance.html#use-the-production-build
        // and some other utility methods
        // You probably want `utils/config` instead.
        NODE_ENV: JSON.stringify(isProductionBuild ? 'production' : 'development')
      })
    }),
    new CopyWebpackPlugin([
      { from: 'src/favicon/favicons' },
      {
        from: `node_modules/monaco-editor/${isProductionBuild ? 'min' : 'dev'}/vs`,
        to: 'vs'
      }
    ]),
    minify && new UglifyJSPlugin({
      sourceMap: true
    }),
    !skipSourceMapUpload && new SentryCliPlugin({
      release: dremioVersion,
      include: outputPath,
      ignore: [
        'vs', // ignore monaco editor sources
        '**/*.css.map'
      ],
      configFile: path.resolve(__dirname, '.sentryclirc'),
      rewrite: true
    })
  ].filter(Boolean),
  resolve: {
    modules: [
      path.resolve(__dirname, 'src'),
      'node_modules',
      path.resolve(__dirname, 'node_modules') // TODO: this is ugly, needed to resolve module dependencies outside of src/ so they can find our main node_modules
    ],
    alias: {
      'dyn-load': dynLoader.path, // ref for std code to ref dynamic componentsd
      '@app': path.resolve(__dirname, 'src'),
      '@root': path.resolve(__dirname),
      'Narwhal-Logo-With-Name-Light': path.resolve(
        isBeta
          ? './src/components/Icon/icons/Narwhal-Logo-With-Name-Light-Beta.svg'
          : './src/components/Icon/icons/Narwhal-Logo-With-Name-Light.svg'
      )
    }
  }
};

module.exports = config;

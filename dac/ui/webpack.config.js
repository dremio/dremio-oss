/*
 * Copyright (C) 2017 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
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
const childProcess = require('child_process');
const webpack = require('webpack');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const ExtractTextPlugin = require('extract-text-webpack-plugin');
const UglifyJSPlugin = require('uglifyjs-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');
const userConfig = require('./webpackUtils/userConfig');

const dynLoader = require('./dynLoader');
dynLoader.applyNodeModulesResolver();

const isProductionBuild = process.env.NODE_ENV === 'production';
const minify = process.env.DREMIO_MINIFY === 'true';
const isBeta = process.env.DREMIO_BETA === 'true';
const isRelease = process.env.DREMIO_RELEASE === 'true';

let devtool = isProductionBuild ? 'source-map' : userConfig.sourceMaps;  // chris says: '#cheap-eval-source-map' is really great for debugging
if (isRelease) {
  // for release, hide the source map
  devtool = 'hidden-source-map';
}

console.info({
  minify,
  isProductionBuild,
  isBeta,
  isRelease,
  dynLoaderPath: dynLoader.path,
  devtool
});

const extractStyles = new ExtractTextPlugin({
  filename: isProductionBuild ? 'style.[contentHash].css' : 'style.css'
});

class BuildInfo {
  apply(compiler) {
    compiler.plugin('compilation', function(compilation) {
      compilation.plugin('html-webpack-plugin-before-html-generation', function(htmlPluginData, callback) {
        // eslint-disable-next-line no-sync
        let commit = childProcess.execSync('git rev-parse HEAD').toString().trim();
        // eslint-disable-next-line no-sync
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
          isReleaseBuild: ${isRelease},
          commit: ${JSON.stringify(commit)},
          ts: "${new Date()}",
          intercomAppId: ${JSON.stringify(isProductionBuild ? '${dremio.config.intercom.appid}' : userConfig.intercomAppId)},
          shouldEnableBugFiling: ${!isProductionBuild || '${dremio.debug.bug.filing.enabled?c}'},
          shouldEnableRSOD: ${!isProductionBuild || '${dremio.debug.rsod.enabled?c}'},
          supportEmailTo: ${JSON.stringify(isProductionBuild ? '${dremio.settings.supportEmailTo}' : 'noreply@dremio.com')},
          supportEmailSubjectForJobs: ${JSON.stringify(isProductionBuild ? '${dremio.settings.supportEmailSubjectForJobs}' : '')},
          outsideCommunicationDisabled: ${isProductionBuild ? '${dremio.settings.outsideCommunicationDisabled?c}' : false},
          subhourAccelerationPoliciesEnabled: ${isProductionBuild ? '${dremio.settings.subhourAccelerationPoliciesEnabled?c}' : false},
          lowerProvisioningSettingsEnabled: ${isProductionBuild ? '${dremio.settings.lowerProvisioningSettingsEnabled?c}' : false},
          tdsMimeType: ${JSON.stringify('${dremio.settings.tdsMimeType}')},
          clusterId: ${JSON.stringify('${dremio.clusterId}')},
          versionInfo: {
            version: ${JSON.stringify('${dremio.versionInfo.version}')},
            buildTime: ${isProductionBuild ? '${dremio.versionInfo.buildtime?c}' : 0},
            commitHash: ${JSON.stringify('${dremio.versionInfo.commit.hash}')},
            commitTime: ${isProductionBuild ? '${dremio.versionInfo.commit.time?c}' : 0}
          }
        }`;

        htmlPluginData.plugin.options.config = config;
        callback(null, htmlPluginData);
      });
    });
  }
}

const loaders = [
  {
    test: /art\/.*\.svg$/,
    use: [
      'babel-loader',
      {
        loader: 'react-svg-loader',
        options: {
          svgo: {
            plugins: [{removeDimensions: true}]
          }
        }
      }
    ]
  },
  {
    test : /\.js$/,
    exclude: /node_modules(?!\/regenerator-runtime|\/redux-saga|\/whatwg-fetch)/,
    include:  [__dirname, dynLoader.path],
    use: [
      {
        loader: 'babel-loader',
        query: {
          // eslint-disable-next-line no-sync
          ...JSON.parse(fs.readFileSync(path.resolve(__dirname, '.babelrc'), 'utf8')),
          cacheDirectory: true
        }
      }
    ]
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
    test: /\.less$/,
    use: extractStyles.extract({
      use: [
        { loader: 'css-loader', options: { importLoaders: 1 } },
        { loader: 'postcss-loader', options: { config: { path: __dirname} } },
        { loader: 'less-loader' }
      ]
    })
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

const plugins = [
  new webpack.BannerPlugin(require(dynLoader.path + '/webpackBanner')),
  extractStyles,
  new webpack.optimize.CommonsChunkPlugin({
    name: 'vendor',
    filename: isProductionBuild ? 'vendor.[hash].js' : 'vendor.js'
  }),
  new HtmlWebpackPlugin({
    template: './src/index.html',
    cache: false, // make sure rebuilds kick BuildInfo too
    files: {
      css: [isProductionBuild ? 'style.[contentHash].css' : 'style.css'],
      js: [isProductionBuild ? 'bundle.[hash].js' : 'bundle.js', isProductionBuild ? 'vendor.[hash].js' : 'vendor.js']
    }
  }),
  new BuildInfo(),
  new webpack.DefinePlugin({
    // This is for React: https://facebook.github.io/react/docs/optimizing-performance.html#use-the-production-build
    // You probably want `utils/config` instead.
    'process.env': { NODE_ENV: JSON.stringify(isProductionBuild ? 'production' : 'development') }
  }),
  new CopyWebpackPlugin([
    { from: 'src/favicon/favicons' },
    {
      from: `node_modules/monaco-editor/${isProductionBuild ? 'min' : 'dev'}/vs`,
      to: 'vs'
    }
  ])
];

if (minify) {
  plugins.push(new UglifyJSPlugin({
    sourceMap: true
  }));
}

const polyfill = [
  './src/polyfills',
  'element-closest',
  'babel-polyfill'
];

const config = {
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
      'react-bootstrap',
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
    path: path.join(__dirname, 'build'),
    filename: isProductionBuild ? 'bundle.[hash].js' : 'bundle.js',
    sourceMapFilename: 'sourcemaps/[file].map'
  },
  module: {
    loaders
  },
  devtool,
  plugins,
  resolve: {
    modules: [
      path.resolve(__dirname, 'src'),
      'node_modules',
      path.resolve(__dirname, 'node_modules') // TODO: this is ugly, needed to resolve module dependencies outside of src/ so they can find our main node_modules
    ],
    alias: {
      'dyn-load': dynLoader.path, // ref for std code to ref dynamic componentsd
      'Narwhal-Logo-With-Name-Light': path.resolve(
        isBeta
          ? './src/components/Icon/icons/Narwhal-Logo-With-Name-Light-Beta.svg'
          : './src/components/Icon/icons/Narwhal-Logo-With-Name-Light.svg'
      )
    }
  }
};

module.exports = config;

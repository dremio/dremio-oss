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
const path = require('path');
const webpack = require('webpack');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');
const SentryCliPlugin = require('@sentry/webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const { getVersion } = require('./scripts/versionUtils');
const dynLoader = require('./dynLoader');
const injectionResolver = require('./scripts/injectionResolver');

dynLoader.applyNodeModulesResolver();
dynLoader.applyTSConfig();

const isProductionBuild = process.env.NODE_ENV === 'production';
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

const babelOptions = {
  configFile: path.resolve(__dirname, '.babelrc.js')
};

console.info({
  dremioVersion,
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


const getTime = date => {
  const dateStr = date.toISOString();
  const tIndex = dateStr.indexOf('T');
  //extract time from ISO string '2019-08-16T19:49:36.337Z'
  return dateStr.substr(tIndex + 1, dateStr.length - tIndex - 2);
};

class BuildInfo {
  apply(compiler) {
    compiler.hooks.compilation.tap('BuildInfo', (compilation) => {
      // Static Plugin interface |compilation |HOOK NAME | register listener
      compilation.hooks.htmlWebpackPluginBeforeHtmlGeneration.tapAsync(
        'BuildInfo', // <-- Set a meaningful name here for stacktraces
        (htmlPluginData, callback) => {

          // because config is relying on freemarker template variables to be interpreted by the server
          // at runtime, config has to be string (and not an object). ${dremio} is a freemaker variable.
          const config = isProductionBuild ? 'JSON.parse(\'${dremio?js_string}\')' : 'null';

          htmlPluginData.plugin.options.config = config;
          callback(null, htmlPluginData);
        }
      );
    });

    compiler.hooks.done.tap('BuildInfo', (stats) => {
      const compilationTime = stats.endTime - stats.startTime;
      const date = new Date(null);
      date.setMilliseconds(compilationTime);
      console.info('Compilation time: ', getTime(date));
    });
  }
}


const babelLoader = {
  loader: 'babel-loader',
  options: {
    ...babelOptions,
    cacheDirectory: true
  }
};

const getStyleLoader = (isCss) => {
  const otherLoaders = [
    !isCss && 'less-loader'
  ].filter(Boolean);
  return {
    test: isCss ? /\.css$/ : /\.less$/,
    use: [
      {
        loader: MiniCssExtractPlugin.loader,
        options: {
          hmr: process.env.NODE_ENV === 'development'
        }
      },
      {
        loader: 'css-loader',
        options: {

          modules: !isCss && {
            mode: 'local',
            localIdentName: '[name]__[local]___[hash:base64:5]'
          },
          // do not use camelCaseOnly here, as composition for classes with '-' in name will be broken
          // For example the following composition will not work:
          // composes: some-selector-with-dash from '~@app/some.less'
          localsConvention: 'camelCase',
          importLoaders: otherLoaders.length
        }
      },
      ...otherLoaders
    ]
  };
};

const rules = [
  {
    test : /\.(js(x)?|ts(x)?)$/,
    exclude: /node_modules(?!\/regenerator-runtime|\/redux-saga|\/whatwg-fetch)/,
    include:  [__dirname, dynLoader.path],
    use: [babelLoader]
  },
  getStyleLoader(false),
  getStyleLoader(true),
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
    test: /\.pattern$/,
    use: {
      loader: 'glob-loader'
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

const getName = (ext) => `[name]${isProductionBuild ? '.[contenthash]' : ''}.${ext}`;
const outFileNameTemplate = getName('js');
const cssFileNameTemplate = getName('css');

const config = {
  // abort process on errors
  bail: true,
  mode: isProductionBuild ? 'production' : 'development',
  entry: {
    app: [
      './src/polyfills',
      path.resolve(__dirname, 'src/index.js')
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
    rules
  },
  devtool,
  plugins: [
    new MiniCssExtractPlugin({
      // Options similar to the same options in webpackOptions.output
      // all options are optional
      filename: cssFileNameTemplate,
      chunkFilename: cssFileNameTemplate,
      ignoreOrder: false // Enable to remove warnings about conflicting order
    }),
    new webpack.BannerPlugin(require(dynLoader.path + '/webpackBanner')),
    new webpack.HashedModuleIdsPlugin(),
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
    new webpack.DefinePlugin({ // todo
      // copy some variables that are required by UI code
      'process.env': [
        'DREMIO_RELEASE',
        'DREMIO_VERSION',
        'EDITION_TYPE',
        'SKIP_SENTRY_STEP',
        'DCS_V2_URL',
        'DCS_V3_URL'
      ].reduce((resultObj, variableToCopy) => {
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
  optimization: {
    runtimeChunk: 'single',
    splitChunks: {
      cacheGroups: {
        vendor: {
          test: /node_modules/,
          chunks: 'initial',
          name: 'vendor',
          enforce: true
        }
      }
    }
  },
  resolve: {
    extensions: ['.js', '.jsx', '.ts', '.tsx', '.json'],
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
    },
    plugins: [new injectionResolver()]
  }
};

module.exports = config;

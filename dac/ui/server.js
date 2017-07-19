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
/*eslint no-console: 0*/

const webpack = require('webpack');
const webpackDevMiddleware = require('webpack-dev-middleware');
const webpackHotMiddleware = require('webpack-hot-middleware');
const proxy = require('http-proxy-middleware');

// -- Probably a better treatment without new
const express = require('express');
const app = express();

const config = require('./webpack.config');
const testConfig = require('./webpack.tests.config');
const userConfig = require('./webpackUtils/userConfig');
const isProductionBuild = process.env.NODE_ENV === 'production';

const stats = { // make it easier to find the error when the build fails
  hash: false,
  version: false,
  timings: false,
  assets: false,
  entrypoints: false,
  chunks: false,
  chunkModules: false,
  modules: false,
  reasons: false,
  depth: false,
  usedExports: false,
  providedExports: false,
  children: false,
  source: false,
  errors: true,
  errorDetails: true,
  warnings: false,
  publicPath: false
};

const port = 3005;
const compiler = webpack(config);
const devMiddleware = webpackDevMiddleware(compiler, {
  noInfo: true,
  publicPath: config.output.publicPath,
  stats,
  // todo: upgrade default reporter so that the error is in the repsonse (and postback to open source)
  watchOptions: {
    // Watching node_modules results in excessive of polling which eats cpu
    ignored: /node_modules/
  }
});
app.use(devMiddleware);

let testMiddleware;
if (!isProductionBuild) {
  const testCompiler = webpack(testConfig);
  testMiddleware = webpackDevMiddleware(testCompiler, {
    publicPath: testConfig.output.publicPath,
    stats
  });
  app.use(testMiddleware);
  app.use(webpackHotMiddleware(testCompiler));
}

app.use(webpackHotMiddleware(compiler)); // todo: what happens in prod mode?

let storedProxy;
let prevAPIOrigin;

// Job profiles load their css/js from /static/*, so redirect those calls as well
app.use(['/api*', '/static/*'], function() {
  const newAPIOrigin = userConfig.live().apiOrigin;
  if (newAPIOrigin !== prevAPIOrigin) {
    storedProxy = proxy({
      target: newAPIOrigin,
      changeOrigin: true,
      logLevel: 'debug',
      ws: true,
      onProxyRes(proxyRes) {
        proxyRes.headers['x-proxied-from'] = newAPIOrigin; // useful for HAR reports
      }
    });
    prevAPIOrigin = newAPIOrigin;
  }

  return storedProxy(...arguments);
});

// todo: this doesn't show dyn-loader tests
app.use(function(req, res, next) {
  if (req.url.indexOf('/unit-tests') !== -1 && testMiddleware) {
    req.url = testConfig.output.publicPath;
    testMiddleware(req, res, next);
  } else {
    req.url = config.output.publicPath;
    devMiddleware(req, res, next);
  }
});

console.log('Building...');

app.listen(port, function(error) {
  if (error) {
    console.error(error);
  }
});

devMiddleware.waitUntilValid(function() {
  console.info('==> 🌎  Listening on port %s. Open up http://localhost:%s/ in your browser.', port, port);
});

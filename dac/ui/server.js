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
/*eslint no-console: 0*/

const fs = require('fs');
const path = require('path');
const webpack = require('webpack');
const webpackDevMiddleware = require('webpack-dev-middleware');
const proxy = require('http-proxy-middleware');

// -- Probably a better treatment without new
const express = require('express');
const app = express();

// Must be before webpack.config.js import
process.env.SKIP_SENTRY_STEP = 'true';

const config = require('./webpack.config');
config.bail = false; // to not fail on compilation error in dev mode



const port = 3005;
const compiler = webpack(config);
const devMiddleware = webpackDevMiddleware(compiler, {
  noInfo: true,
  output: config.output,
  // todo: upgrade default reporter so that the error is in the repsonse (and postback to open source)
  watchOptions: {
    // Watching node_modules results in excessive of polling which eats cpu
    ignored: /node_modules/
  }
});
app.use(devMiddleware);

let storedProxy;
let prevAPIOrigin;
const readServerSettings = () => {
  return JSON.parse(fs.readFileSync( // eslint-disable-line no-sync
    path.resolve(__dirname, 'server.config.json'),
    'utf8'
  ));
};

// Job profiles load their css/js from /static/*, so redirect those calls as well
app.use(['/api*', '/static/*'], function() {
  // todo it would be nice to enforce server and web socket to use the same origin.
  // See https://github.com/dremio/dremio/blob/64ada2028fb042e6b3a035b9ef64814218095e30/oss/dac/ui/src/constants/Api.js#L22
  const newAPIOrigin = readServerSettings().apiOrigin;
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
  req.url = config.output.publicPath;
  devMiddleware(req, res, next);
});

console.log('Building…');

app.listen(port, function(error) {
  if (error) {
    console.error(error);
  }
});

devMiddleware.waitUntilValid(function() {
  console.info('==> 🌎  Listening on port %s. Open up http://localhost:%s/ in your browser.', port, port);
});

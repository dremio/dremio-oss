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

const fs = require("fs");
const path = require("path");
const webpack = require("webpack");
const webpackDevMiddleware = require("webpack-dev-middleware");
const { createProxyMiddleware } = require("http-proxy-middleware");

// -- Probably a better treatment without new
const express = require("express");
const app = express();

// Must be before webpack.config.js import
process.env.SKIP_SENTRY_STEP = "true";

const config = require("./webpack.config");
config.bail = false; // to not fail on compilation error in dev mode

const port = 3005;
const compiler = webpack(config);
const devMiddleware = webpackDevMiddleware(compiler);
app.use(devMiddleware);

const readServerSettings = () => {
  return JSON.parse(
    fs.readFileSync(
      // eslint-disable-line no-sync
      path.resolve(__dirname, "server.config.json"),
      "utf8"
    )
  );
};

const settings = readServerSettings();
console.log(`API requests => ` + "\x1b[36m" + settings.apiOrigin + "\x1b[0m");

const getApiOrigin = (pattern) => {
  return settings[pattern.origin || "apiOrigin"] + (pattern.subDomain || "");
};

//Unused for now: The following can be used for proxying directly to a nessie
// const getNessieDirectOptions = (pattern) => {
//   if (pattern.origin && pattern.origin === 'nessieApi' && process.env.NESSIE_DIRECT_URL) {
//     return {
//       pathRewrite: { '^/nessie/projects/.*?/': '/api/v1/' },
//       target: process.env.NESSIE_DIRECT_URL
//     };
//   } else {
//     return {};
//   }
// };
const TIMEOUT = 480000;
// Job profiles load their css/js from /static/*, so redirect those calls as well
settings.proxyPatterns.forEach((p) => {
  const target = p.target || getApiOrigin(p);
  app.use(
    p.patterns,
    createProxyMiddleware({
      ...p,
      target,
      changeOrigin: true,
      logLevel: "warn",
      proxyTimeout: TIMEOUT,
      timeout: TIMEOUT,
      onProxyRes(proxyRes, _req, res) {
        if (res.writableEnded) {
          return;
        }
        proxyRes.headers["x-proxied-from"] = target; // useful for HAR reports
      },
      onError(err, req, res) {
        console.error(req.path);
        console.error(err);

        if (res.writableEnded) {
          return;
        }

        res.end(`Error occurred while trying to proxy`);
      },
    })
  );
});

// todo: this doesn't show dyn-loader tests
app.use(function (req, res, next) {
  req.url = config.output.publicPath;
  devMiddleware(req, res, next);
});

app.listen(port, function (error) {
  if (error) {
    console.error(error);
  }
});

devMiddleware.waitUntilValid(function () {
  console.log(
    "\n" +
      "\x1b[36m" +
      `Listening on port ${port}` +
      "\x1b[0m" +
      (process.env.ENABLE_MSW
        ? "\x1b[35m" + "\n[MSW] Mocking enabled" + "\x1b[0m"
        : "") +
      `\nOpen http://localhost:${port} in your browser`
  );
});

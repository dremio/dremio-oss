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
import * as env from "env-var";

const DEV_DCS_TARGET = env
  .get("DEV_DCS_TARGET")
  .default("https://app.dev.dremio.site")
  .asString();
const DEV_NESSIE_TARGET = env
  .get("DEV_NESSIE_TARGET")
  .default(DEV_DCS_TARGET.replace("app.", "nessie."))
  .asString();
const DEV_NESSIE_TARGET_PREFIX = env
  .get("DEV_NESSIE_TARGET_PREFIX")
  .default("true")
  .asBool();
const DEV_APP_TARGET = env
  .get("DEV_APP_TARGET")
  .default("http://automaster.drem.io:9047")
  .asString();

export const proxy = [
  {
    context: "/apiv2/socket",
    target: DEV_APP_TARGET,
    changeOrigin: true,
    ws: true,
  },
  {
    context: ["/api"],
    target: DEV_APP_TARGET,
    changeOrigin: true,
  },
  {
    context: ["/nessie-proxy/v2"],
    target: DEV_APP_TARGET,
    changeOrigin: true,
  },
  {
    context: ["/nessieV1"],
    target: DEV_NESSIE_TARGET,
    changeOrigin: true,
    pathRewrite: { "^/nessieV1/": DEV_NESSIE_TARGET_PREFIX ? "/v1/" : "/" },
  },
  {
    context: ["/nessie"],
    target: DEV_NESSIE_TARGET,
    changeOrigin: true,
    pathRewrite: { "^/nessie/": DEV_NESSIE_TARGET_PREFIX ? "/v2/" : "/" },
  },
  {
    context: ["/support"],
    target: DEV_DCS_TARGET.replace("app.", "support."),
    changeOrigin: true,
  },
  {
    context: ["/ui"],
    target: DEV_DCS_TARGET,
    changeOrigin: true,
  },
  {
    context: ["/v0"],
    target: DEV_DCS_TARGET.replace("app.", "api."),
    changeOrigin: true,
  },
];

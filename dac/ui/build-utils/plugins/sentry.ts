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
import { join } from "path";
import { sentryWebpackPlugin } from "@sentry/webpack-plugin";
import { dremioVersion } from "../version";
import { output } from "../output";
export const sentryPlugin =
  process.env.SKIP_SENTRY_STEP !== "true" &&
  sentryWebpackPlugin({
    telemetry: false,
    release: {
      name: dremioVersion,
    },
    authToken: process.env.SENTRY_AUTH_TOKEN,
    project: "frontend",
    org: "dremio",
    sourcemaps: {
      filesToDeleteAfterUpload: [join(output.path, "sourcemaps/**/*.map")],
    },
  });

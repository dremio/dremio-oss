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
import "core-js/full/array/from-async";
import { additionalSetup } from "./additionalSetup";
import { createRoot } from "react-dom/client";

import sentryUtil from "utils/sentryUtil";
import startup from "dyn-load/startup";
import setupMetrics from "@inject/setupMetrics";
import "@inject/vendor/segment";

import "dremio-ui-lib/dist-themes/base/assets/fonts/inter-ui/inter.css";
import "dremio-ui-lib/dist-themes/base/assets/fonts/FiraCode/FiraCode.css";
import "dremio-ui-lib/dist-themes/dremio/index.css";
import "dremio-ui-lib/dist/index.css";
import "./index.scss";
import "./main.less";
import "./uiTheme/css/typography.css";
import Root from "./containers/Root";
import configureStore from "./store/configureStore";
import { ErrorBoundary } from "#oss/components/ErrorBoundary/ErrorBoundary";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";
import $ from "jquery";
import { QueryClientProvider } from "@tanstack/react-query";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import { queryClient } from "./queryClient";
import { applyTheme } from "./theme";

setupMetrics();

window.laDeprecated = (key) => {
  return key;
};

sentryUtil.install();

const store = configureStore();

/**
 * We need to work on finding all of the places that jquery is
 * implicitly used from the window instead of explicitly through an import and
 * fix them. For production builds we shouldn't risk anything, but for dev builds
 * we can disable the globals now and fix any occurrences as we come across them.
 */
if (process.env.NODE_ENV !== "development") {
  window.$ = $;
}

const initApp = async () => {
  applyTheme();

  if (process.env.ENABLE_MSW === "true") {
    await (await import("@inject/setupMsw")).browserMocks();
  }

  startup.run();
  await additionalSetup();

  createRoot(document.getElementById("root")).render(
    <ErrorBoundary
      title={getIntlContext().t("Common.Errors.UnexpectedError.Root")}
    >
      <QueryClientProvider client={queryClient}>
        <Root store={store} />
        <ReactQueryDevtools />
      </QueryClientProvider>
    </ErrorBoundary>,
  );
};

initApp();

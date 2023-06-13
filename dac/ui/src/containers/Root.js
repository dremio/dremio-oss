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
import { useEffect } from "react";
import PropTypes from "prop-types";

import { RawIntlProvider } from "react-intl";
import { Provider } from "react-redux";

import { Router, browserHistory } from "react-router";
import { syncHistoryWithStore } from "react-router-redux";
import { useProjectContext } from "@inject/utils/storageUtils/localStorageUtils";
import routes from "routes";

import useLoadLocale from "utils/locale/useLoadLocale";
import { add } from "utils/storageUtils/localStorageListener";
import { setUserState } from "@app/actions/account";
import { intl } from "@app/utils/intl";
import { MantineProvider } from "@mantine/core";
import { mantineTheme } from "dremio-ui-lib/components";

import { NetworkConnectivityBanner } from "dremio-ui-common/components/NetworkConnectivityBanner.js";
import { ErrorBoundary } from "@app/components/ErrorBoundary/ErrorBoundary";

import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

function Root({ store }) {
  const history = syncHistoryWithStore(browserHistory, store);
  const projectContext = useProjectContext();
  //Re-render routes when project context changes
  const renderKey = projectContext?.id || "root";

  //DX-45764 - Synchronize user object in redux when another tab has logged in
  useEffect(
    () =>
      add((event) => {
        const val = event.newValue;
        if (store != null && event.key === "user" && val != null) {
          store.dispatch(setUserState(JSON.parse(val)));
        }
      }),
    [store]
  );

  const localeLoading = useLoadLocale();
  if (localeLoading) return null;

  return (
    <ErrorBoundary
      title={getIntlContext().t("Common.Errors.UnexpectedError.Root")}
    >
      <MantineProvider theme={mantineTheme}>
        <RawIntlProvider value={intl}>
          <Provider store={store}>
            <NetworkConnectivityBanner />
            <Router key={renderKey} history={history}>
              {routes(store.dispatch, projectContext)}
            </Router>
          </Provider>
        </RawIntlProvider>
      </MantineProvider>
    </ErrorBoundary>
  );
}
Root.propTypes = {
  store: PropTypes.object.isRequired,
};
export default Root;

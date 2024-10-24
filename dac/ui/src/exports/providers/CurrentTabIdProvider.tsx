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

import { useCallback, useMemo, type PropsWithChildren } from "react";
import { currentTabIdContext } from "dremio-ui-common/sonar/SqlRunnerSession/currentTabIdContext.js";
import { withRouter, type WithRouterProps } from "react-router";

export const CurrentTabIdProvider = withRouter(
  (props: PropsWithChildren & WithRouterProps) => {
    const scriptId = (props.location.query.scriptId || null) as string | null;
    const setCurrentTabId = useCallback(
      (tabId: string) => {
        const nextLocation = props.location;
        nextLocation.query.scriptId = tabId;
        props.router.replace(nextLocation);
      },
      [props.location, props.router],
    );

    return (
      <currentTabIdContext.Provider
        value={useMemo(() => {
          return {
            currentTabId: scriptId,
            setCurrentTabId,
          };
        }, [scriptId, setCurrentTabId])}
      >
        {props.children}
      </currentTabIdContext.Provider>
    );
  },
);

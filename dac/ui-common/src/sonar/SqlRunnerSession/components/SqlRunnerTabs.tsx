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
import {
  TabListWrapper,
  TabListTab,
  IconButton,
  Skeleton,
} from "dremio-ui-lib/components";
import { useSqlRunnerSession } from "../providers/useSqlRunnerSession";
import {
  ScriptsResource,
  MAXIMUM_SCRIPTS,
} from "../../scripts/resources/ScriptsResource";
import { useEffect, useMemo, useRef } from "react";
import { useScript } from "../../scripts/providers/useScript";
import { getIntlContext } from "../../../contexts/IntlContext";
import { useScriptsCount } from "../../scripts/providers/useScriptsCount";
import { throttle } from "lodash";

type SqlRunnerTabsProps = {
  canClose?: () => boolean;
  onNewTabCreated?: () => void;
  onTabSelected?: (tabId: string) => void;
  onTabClosed?: (tabId: string) => void;
  tabActions: (tabId: string) => JSX.Element | JSX.Element[];
};

const SqlRunnerTab = (props: {
  onTabSelected: () => void;
  onTabClosed?: () => void;
  selected: boolean;
  tabId: string;
  getMenuItems?: () => JSX.Element | JSX.Element[];
}) => {
  const script = useScript(props.tabId);
  return (
    <TabListTab
      aria-controls=""
      aria-selected={props.selected}
      id={`sql__${props.tabId}`}
      onClose={props.onTabClosed}
      onSelected={props.onTabSelected}
      getMenuItems={props.getMenuItems as any}
    >
      {script?.name || <Skeleton width="22ch" />}
    </TabListTab>
  );
};

export const SqlRunnerTabs = (props: SqlRunnerTabsProps) => {
  const sqlRunnerSession = useSqlRunnerSession();

  useEffect(() => {
    ScriptsResource.fetch();
  }, []);

  const canCloseTabs =
    typeof props.canClose === "function" ? props.canClose() : true;

  const onNewTabCreatedRef = useRef(props.onNewTabCreated);
  onNewTabCreatedRef.current = props.onNewTabCreated;

  const newTabRef = useRef(sqlRunnerSession.newTab);
  newTabRef.current = sqlRunnerSession.newTab;

  const handleNewTabButton = useMemo(() => {
    return throttle(
      () =>
        newTabRef
          .current()
          .then(() => {
            onNewTabCreatedRef.current?.();
            return;
          })
          .catch((e) => {
            console.error(e);
          }),
      1000
    );
  }, []);

  const { t } = getIntlContext();

  const scriptsCount = useScriptsCount();

  return (
    <div
      className="flex flex-row overflow-hidden"
      style={{ borderBottom: "1px solid #e9edf0" }}
    >
      <TabListWrapper
        aria-label="Scripts"
        tabControls={
          <IconButton
            tooltip={
              <div className="dremio-prose" style={{ maxWidth: "300px" }}>
                {scriptsCount === MAXIMUM_SCRIPTS
                  ? t("Sonar.Scripts.LimitReached")
                  : "New tab"}
              </div>
            }
            onClick={handleNewTabButton}
          >
            {/* @ts-ignore */}
            <dremio-icon name="interface/plus" alt=""></dremio-icon>
          </IconButton>
        }
      >
        {sqlRunnerSession?.scriptIds?.map((tabId) => {
          const selected = tabId === sqlRunnerSession.currentScriptId;
          return (
            <SqlRunnerTab
              tabId={tabId}
              selected={selected}
              key={tabId}
              onTabSelected={() => {
                if (!selected) {
                  props.onTabSelected?.(tabId);
                }
              }}
              getMenuItems={
                selected && ((() => props.tabActions(tabId)) as any)
              }
              onTabClosed={
                canCloseTabs
                  ? () => {
                      props.onTabClosed?.(tabId);
                    }
                  : undefined
              }
            />
          );
        })}
      </TabListWrapper>
    </div>
  );
};

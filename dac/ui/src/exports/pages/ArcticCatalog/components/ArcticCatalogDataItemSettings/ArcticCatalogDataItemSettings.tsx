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
import { useState } from "react";
//@ts-ignore
import { IconButton } from "dremio-ui-lib";
import Immutable from "immutable";
import { useIntl } from "react-intl";
//@ts-ignore
import { ModalContainer, DialogContent } from "dremio-ui-lib/components";
import { optimizationScheduleResource } from "@app/exports/resources/OptimizationScheduleResource";
import NavPanel from "components/Nav/NavPanel";
import { ArcticTableOptimizationForm } from "./ArcticTableOptimization/ArcticTableOptimization";
import { type EngineStateRead } from "@app/exports/endpoints/ArcticCatalogs/Configuration/CatalogConfiguration.types";
import * as classes from "./ArcticCatalogDataItemSettings.module.less";

type ArcticCatalogDataItemSettingsProps = {
  title: string;
  reference: string;
  catalogId: string;
  tableId: string;
  optimizationModal: {
    open: () => void;
    close: () => void;
    isOpen: boolean;
    children?: Element;
  };
  configState?: EngineStateRead;
};
export const ArcticCatalogDataItemSettings = ({
  reference,
  tableId,
  catalogId,
  optimizationModal,
  title,
  configState,
}: ArcticCatalogDataItemSettingsProps) => {
  const [tab, setTab] = useState("TableOptimization");

  const renderTab = () => {
    switch (tab) {
      case "TableOptimization":
        return (
          <ArcticTableOptimizationForm
            configState={configState}
            reference={reference}
            tableId={tableId}
            catalogId={catalogId}
          />
        );
    }
  };

  const { formatMessage } = useIntl();

  return (
    <>
      <span className="settings-icon-container">
        <IconButton
          aria-label="Optimization Settings"
          onClick={optimizationModal.open}
        >
          <dremio-icon name="interface/settings" alt="Settings Button" />
        </IconButton>
      </span>
      <ModalContainer
        {...optimizationModal}
        close={() => {
          optimizationModal.close();
          optimizationScheduleResource.reset();
        }}
      >
        <DialogContent
          title={formatMessage({ id: "Dataset.Settings.for" }, { title })}
          toolbar={
            <IconButton
              aria-label="Close"
              onClick={() => {
                optimizationModal.close();
                optimizationScheduleResource.reset();
              }}
            >
              <dremio-icon name="interface/close-small" />
            </IconButton>
          }
        >
          <div className={classes["container"]} data-qa="dataset-settings">
            <NavPanel
              showSingleTab
              tabs={Immutable.OrderedMap([
                [
                  "TableOptimization",
                  formatMessage({
                    id: "Arctic.Table.Optimization",
                  }),
                ],
              ])}
              changeTab={(tab: string) => setTab(tab)}
              activeTab={tab}
            />
            {renderTab()}
          </div>
        </DialogContent>
      </ModalContainer>
    </>
  );
};

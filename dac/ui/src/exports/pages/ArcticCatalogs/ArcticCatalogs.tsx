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
  Button,
  Page,
  useSegmentedControl,
  useModalContainer,
  ModalContainer,
  Spinner,
} from "dremio-ui-lib/dist-esm";
import { ArcticCatalogsList } from "../../components/ArcticCatalogsList/ArcticCatalogsList";
import { ArcticCatalogsTable } from "../../components/ArcticCatalogsTable/ArcticCatalogsTable";
import { TableViewSwitcher } from "../../components/TableViewSwitcher/TableViewSwitcher";
import { SimplePageHeader } from "../../components/PageHeaders/SimplePageHeader";
import { browserHistory } from "react-router";
import * as PATHS from "../../paths";
import { type ArcticCatalog } from "../../endpoints/ArcticCatalogs/ArcticCatalog.type";
import { NewArcticCatalogDialog } from "../../components/NewArcticCatalogDialog/NewArcticCatalogDialog";
import NavCrumbs from "@inject/components/NavCrumbs/NavCrumbs";
import { ArcticSideNav } from "../../components/SideNav/ArcticSideNav";
import { ErrorBoundary } from "@app/components/ErrorBoundary/ErrorBoundary";
import { ArcticCatalogsResource } from "@app/exports/resources/ArcticCatalogsResource";
import { ResourceSwitch } from "@app/exports/components/ResourceSwitch/ResourceSwitch";
import { useResourceStatus } from "smart-resource/react";
import { FeatureSwitch } from "@app/exports/components/FeatureSwitch/FeatureSwitch";
import { ARCTIC_CATALOG_CREATION } from "@app/exports/flags/ARCTIC_CATALOG_CREATION";

const handleArcticCatalogCreation = (createdCatalog: ArcticCatalog) => {
  browserHistory.push(
    PATHS.arcticCatalog({ arcticCatalogId: createdCatalog.id })
  );
};

const ERROR_TITLE =
  "Something went wrong when we tried to show the list of Arctic Catalogs.";

const renderCatalogsList = ({ viewMode, arcticCatalogs }: any) => {
  if (arcticCatalogs?.length === 0) {
    return <div className="dremio-empty-overlay">No Items</div>;
  }

  return (
    <div style={{ marginTop: "var(--dremio--spacing--2)" }}>
      {viewMode === "card" && <ArcticCatalogsList catalogs={arcticCatalogs} />}
      {viewMode === "grid" && <ArcticCatalogsTable catalogs={arcticCatalogs} />}
    </div>
  );
};

export const ArcticCatalogs = (): JSX.Element => {
  const viewMode = useSegmentedControl<"card" | "grid">("card");
  const newArcticCatalog = useModalContainer();
  const arcticCatalogsStatus = useResourceStatus(ArcticCatalogsResource);

  return (
    <div className="page-content">
      <ArcticSideNav actions={<></>} />
      <Page
        header={
          <>
            <NavCrumbs disableCatalogSelector disableCurrentLocCrumb />
            <SimplePageHeader
              title={
                <span>
                  Arctic Catalogs
                  {arcticCatalogsStatus === "pending" && <Spinner />}
                </span>
              }
              toolbar={
                <>
                  <TableViewSwitcher {...viewMode} />
                  <FeatureSwitch
                    flag={ARCTIC_CATALOG_CREATION}
                    renderEnabled={() => (
                      <Button variant="primary" onClick={newArcticCatalog.open}>
                        <dremio-icon name="interface/add" alt=""></dremio-icon>
                        Add Catalog
                      </Button>
                    )}
                  />
                </>
              }
            />
          </>
        }
      >
        <ModalContainer {...newArcticCatalog}>
          <NewArcticCatalogDialog
            onCancel={newArcticCatalog.close}
            onSuccess={handleArcticCatalogCreation}
          />
        </ModalContainer>
        <ErrorBoundary title={ERROR_TITLE}>
          <ResourceSwitch
            errorMessage={ERROR_TITLE}
            resource={ArcticCatalogsResource}
            renderPending={(arcticCatalogs) =>
              renderCatalogsList({
                viewMode: viewMode.value,
                arcticCatalogs,
              })
            }
            renderSuccess={(arcticCatalogs) =>
              renderCatalogsList({ viewMode: viewMode.value, arcticCatalogs })
            }
          />
        </ErrorBoundary>
      </Page>
    </div>
  );
};

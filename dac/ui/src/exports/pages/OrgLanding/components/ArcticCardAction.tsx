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

import * as PATHS from "../../../paths";
import { browserHistory, Link } from "react-router";
import { useArcticCatalogs } from "../../../providers/useArcticCatalogs";
import classes from "./ServicesSection.less";
import {
  Button,
  ModalContainer,
  useModalContainer,
} from "dremio-ui-lib/dist-esm";
import { NewArcticCatalogDialog } from "../../../components/NewArcticCatalogDialog/NewArcticCatalogDialog";
import { ARCTIC_CATALOG_CREATION } from "@app/exports/flags/ARCTIC_CATALOG_CREATION";
import { FeatureSwitch } from "@app/exports/components/FeatureSwitch/FeatureSwitch";

const handleArcticCatalogCreation = (createdCatalog: any) => {
  browserHistory.push(
    PATHS.arcticCatalog({ arcticCatalogId: createdCatalog.id })
  );
};

export const ArcticCardAction = () => {
  const newArcticCatalog = useModalContainer();
  const [arcticCatalogs] = useArcticCatalogs();

  let action = (
    <Link
      to={PATHS.arcticCatalogs()}
      className={classes["services-section__catalog-link"]}
    >
      <dremio-icon name="interface/arrow-right"></dremio-icon> See all Arctic
      Catalogs
    </Link>
  );

  if (arcticCatalogs?.length === 0) {
    action = (
      <FeatureSwitch
        flag={ARCTIC_CATALOG_CREATION}
        renderEnabled={() => (
          <Button
            variant="primary"
            style={{ marginInline: "auto" }}
            onClick={newArcticCatalog.open}
          >
            <dremio-icon name="interface/add" alt=""></dremio-icon> Add Arctic
            Catalog
          </Button>
        )}
      />
    );
  }

  return (
    <>
      <div style={{ textAlign: "left" }}>
        <ModalContainer {...newArcticCatalog}>
          <NewArcticCatalogDialog
            onCancel={newArcticCatalog.close}
            onSuccess={handleArcticCatalogCreation}
          />
        </ModalContainer>
      </div>

      {action}
    </>
  );
};

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
  ModalContainer,
  Section,
  useModalContainer,
} from "dremio-ui-lib/dist-esm";
import { useCallback } from "react";
import { browserHistory } from "react-router";
import * as PATHS from "../../../paths";
import { addNotification } from "actions/notification";
import { store } from "@app/store/store";
import { DeleteArcticCatalogDialog } from "../../../components/DeleteArcticCatalogDialog/DeleteArcticCatalogDialog";
import { ArcticCatalog } from "@app/exports/endpoints/ArcticCatalogs/ArcticCatalog.type";
import { removeEntry } from "@app/actions/nessie/nessie";

export const DeleteSection = (props: { catalog: ArcticCatalog }) => {
  const catalogName = props.catalog?.name;
  const handleArcticCatalogDeletion = ({
    catalogName,
  }: {
    catalogName: string;
  }) => {
    store.dispatch(
      addNotification(
        `Arctic catalog ${catalogName} has been deleted.`,
        "success"
      )
    );
    store.dispatch(removeEntry(catalogName)); //Remove entry from Nessie state
    browserHistory.push(PATHS.arcticCatalogs());
  };
  const deleteConfirmationModal = useModalContainer();
  const handleDeleteSuccess = useCallback(
    () =>
      handleArcticCatalogDeletion({
        catalogName: catalogName as string,
      }),
    [catalogName]
  );
  return (
    <Section
      title={
        <h2 style={{ fontSize: "14px", fontWeight: 400 }}>Delete Catalog</h2>
      }
    >
      <div className="dremio-prose">
        <p className="dremio-typography-small dremio-typography-less-important">
          The list of curated branches, tags, folders, views, tables will be
          deleted and unrecoverable. However, the data in tables and view will
          still exist in your object store.
        </p>
        <Button
          variant="secondary-danger"
          onClick={deleteConfirmationModal.open}
        >
          Delete Catalog
        </Button>
        <ModalContainer {...deleteConfirmationModal}>
          <DeleteArcticCatalogDialog
            onCancel={deleteConfirmationModal.close}
            onSuccess={handleDeleteSuccess}
            catalogName={props.catalog.name}
            catalogId={props.catalog.id}
          />
        </ModalContainer>
      </div>
    </Section>
  );
};

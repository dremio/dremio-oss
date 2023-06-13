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
import { useCallback, useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import { browserHistory } from "react-router";
import { FormattedMessage } from "react-intl";

import {
  Button,
  ModalContainer,
  useModalContainer,
} from "dremio-ui-lib/components";
import AddToSonarDialogContent from "./components/AddToSonarDialogContent/AddToSonarDialogContent";
import { ArcticCatalog } from "@app/exports/endpoints/ArcticCatalogs/ArcticCatalog.type";
import { SonarProject } from "@app/exports/endpoints/SonarProjects/listSonarProjects";
import { handleSonarProjectChange } from "@app/utils/projects";
import * as commonPaths from "dremio-ui-common/paths/common.js";

//@ts-ignore
import { fetchOrganizations } from "@inject/actions/account";

function AddToSonarButton({ catalog }: { catalog: ArcticCatalog }) {
  const addToSonarModal = useModalContainer();
  const dispatch = useDispatch();
  const orgInfo = useSelector((state: any) =>
    state.account.get("organizationInfo")
  );

  useEffect(() => {
    if (!orgInfo) dispatch(fetchOrganizations());
  }, [orgInfo, dispatch]);

  const defaultProjectId = orgInfo?.defaultProjectId;

  const onSuccess = useCallback(
    (project: SonarProject) => {
      setTimeout(() => {
        addToSonarModal.close();
        handleSonarProjectChange(project, () =>
          browserHistory.push(
            commonPaths.source.link({
              projectId: project.id,
              resourceId: catalog.name,
            })
          )
        );
      }, 500);
    },
    [addToSonarModal, catalog.name]
  );

  return (
    <>
      <Button
        onClick={addToSonarModal.open}
        variant="tertiary"
        pending={!orgInfo}
      >
        <FormattedMessage id="ArcticCatalog.AddToSonarProjectDialog.ButtonTitle" />
      </Button>
      <ModalContainer {...addToSonarModal}>
        <AddToSonarDialogContent
          defaultProjectId={defaultProjectId}
          catalog={catalog}
          onSuccess={onSuccess}
          onCancel={addToSonarModal.close}
        />
      </ModalContainer>
    </>
  );
}

export default AddToSonarButton;

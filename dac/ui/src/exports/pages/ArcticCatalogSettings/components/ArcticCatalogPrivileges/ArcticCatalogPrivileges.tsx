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
import { useIntl } from "react-intl";
import { useResourceSnapshot } from "smart-resource/react";
import { useDispatch } from "react-redux";

import { SimplePageHeader } from "@app/exports/components/PageHeaders/SimplePageHeader";
import { Page } from "dremio-ui-lib/components";
import { GrantForGet } from "@app/exports/endpoints/ArcticCatalogGrants/ArcticCatalogGrants.types";
import { ArcticCatalogGrantsResource } from "@app/exports/resources/ArcticCatalogGrantsResource";
import {
  prepareCatalogPrivilegesRequestBody,
  CATALOG_LEVEL_NAME_COL,
  CATALOG_LEVEL_PRIVILEGES_TOOLTIP_IDS,
} from "./arctic-catalog-privileges-utils";
import { addNotification } from "@app/actions/notification";
import { modifyArcticCatalogGrants } from "@app/exports/endpoints/ArcticCatalogGrants/modifyArcticCatalogGrants";
import PrivilegesPage, {
  GrantObject,
} from "@app/exports/components/PrivilegesPage/PrivilegesPage";
import { ArcticCatalogOwnershipResource } from "@app/exports/resources/ArcticCatalogOwnershipResource";
import { modifyCatalogOwnership } from "@app/exports/endpoints/ArcticCatalogOwnership/modifyCatalogOwnership";
import { CatalogOwnershipInfo } from "@app/exports/endpoints/ArcticCatalogOwnership/getCatalogOwnership";
import { RouteComponentProps } from "react-router";

import * as classes from "./ArcticCatalogPrivileges.module.less";

const ArcticCatalogPrivileges = (props: RouteComponentProps<any, any>) => {
  const arcticCatalogId = props?.params?.arcticCatalogId;
  const intl = useIntl();
  const dispatch = useDispatch();
  const [granteeData, granteeErr] = useResourceSnapshot(
    ArcticCatalogGrantsResource
  );
  const [ownership]: readonly [CatalogOwnershipInfo | null, any] =
    useResourceSnapshot(ArcticCatalogOwnershipResource);

  // Fetch data on initial render
  useEffect(() => {
    ArcticCatalogGrantsResource.fetch(arcticCatalogId);
    ArcticCatalogOwnershipResource.fetch(arcticCatalogId);
  }, [arcticCatalogId]);

  const onSubmit = async (
    formValues: any,
    tableItems: GrantObject[],
    deletedTableItems: GrantObject[],
    cachedData: any,
    setDirtyStateForLeaving: () => void
  ) => {
    const preparedResults = prepareCatalogPrivilegesRequestBody(
      tableItems as GrantForGet[],
      deletedTableItems as GrantForGet[],
      granteeData,
      formValues,
      cachedData
    );

    if (preparedResults.length === 0) {
      dispatch(
        addNotification(
          intl.formatMessage({ id: "Admin.Privileges.EmptyGrantsError" }),
          "error"
        )
      );
      return;
    }

    try {
      // Update the items, and show notification for any failed grants
      await modifyArcticCatalogGrants(arcticCatalogId, {
        tag: granteeData?.tag ?? "",
        grants: preparedResults,
      });
      dispatch(
        addNotification(
          intl.formatMessage({ id: "Privileges.Success" }),
          "success"
        )
      );
      ArcticCatalogGrantsResource.fetch(arcticCatalogId);
      // TODO: Refetch catalog privileges
      // dispatch(fetchOrgPrivileges());
      setDirtyStateForLeaving();
    } catch (e: any) {
      const message = e?.responseBody?.errorMessage?.includes(
        "Tag does not match"
      )
        ? intl.formatMessage({ id: "Privileges.Update.Error" })
        : e?.responseBody?.errorMessage;
      dispatch(addNotification(message, "error"));
    }
  };

  const handleUpdateOwnership = async (id: string) => {
    try {
      await modifyCatalogOwnership(arcticCatalogId, { owner: id });
      ArcticCatalogOwnershipResource.fetch(arcticCatalogId);
    } catch (e) {
      //
    }
  };

  return (
    <Page
      className={classes["arctic-catalog-privileges"]}
      header={
        <>
          <SimplePageHeader
            title={
              <div className={classes["page-header"]}>
                <dremio-icon name="interface/privilege"></dremio-icon>{" "}
                {intl.formatMessage({
                  id: "Common.Privileges",
                })}
              </div>
            }
          />
        </>
      }
    >
      <PrivilegesPage
        granteeData={granteeData}
        granteeError={granteeErr?.responseBody?.errorMessage}
        isLoading={!granteeData && !granteeErr}
        nameColumnLabel={CATALOG_LEVEL_NAME_COL}
        privilegeTooltipIds={CATALOG_LEVEL_PRIVILEGES_TOOLTIP_IDS}
        entityOwnerId={ownership?.owner}
        handleUpdateOwnership={handleUpdateOwnership}
        onSubmit={onSubmit}
      />
    </Page>
  );
};

export default ArcticCatalogPrivileges;

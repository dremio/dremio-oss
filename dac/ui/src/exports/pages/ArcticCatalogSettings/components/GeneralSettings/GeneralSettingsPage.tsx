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
import { Section } from "dremio-ui-lib/components";
import { TextInput } from "@mantine/core";
import { useIntl } from "react-intl";

import { ConnectList } from "../../ConnectList";
import { DeleteSection } from "../DeleteSection";
import { LabelWithCopyButton } from "../LabelWithCopyButton";

import { getUserId } from "../../../../utilities/getUserId";
import { useUserHasAdminRole } from "../../../../utilities/useUserHasAdminRole";
import { Page } from "dremio-ui-lib/components";
import { SimplePageHeader } from "../../../../components/PageHeaders/SimplePageHeader";

import * as classes from "./GeneralSettingsPage.module.less";

function GeneralSettingsPage(props: any) {
  const id = getUserId();
  const [userHasAdminRole, , status] = useUserHasAdminRole({ id });
  const { formatMessage } = useIntl();

  if (!props.catalog) return null;
  return (
    <Page
      header={
        <>
          <SimplePageHeader
            className={classes["page-header"]}
            title={
              <>
                <dremio-icon name="interface/information"></dremio-icon>{" "}
                {formatMessage({
                  id: "Catalog.Settings.GeneralInformation",
                })}
              </>
            }
          />
        </>
      }
    >
      <div className="dremio-prose">
        <Section>
          <TextInput
            label={formatMessage({ id: "Catalog.Settings.CatalogName" })}
            disabled
            value={props.catalog.name}
          />
          <TextInput
            label={
              <LabelWithCopyButton
                label="Catalog.Settings.Id"
                copyText={props.catalog.id}
                copyTitle={formatMessage({
                  id: "Catalog.Settings.Copy.Id",
                })}
              />
            }
            disabled
            value={props.catalog.id}
          />
          <TextInput
            label={
              <LabelWithCopyButton
                label="Catalog.Settings.Endpoint"
                copyText={props.catalog.nessieEndpoint}
                copyTitle={formatMessage({
                  id: "Catalog.Settings.Copy.Endpoint",
                })}
              />
            }
            disabled
            value={props.catalog.nessieEndpoint}
          />
          <h2
            style={{
              fontSize: "14px",
              fontWeight: 400,
              marginTop: "var(--dremio--spacing--4)",
              marginBottom: "var(--dremio--spacing--1)",
            }}
          >
            Connect from SQL Engines
          </h2>
          <ConnectList catalog={props.catalog} />
        </Section>
        {(!id ||
          (status === "success" && userHasAdminRole) ||
          id === props.catalog.ownerId) && (
          <>
            <hr />
            <DeleteSection catalog={props.catalog} />
          </>
        )}
      </div>
    </Page>
  );
}
export default GeneralSettingsPage;

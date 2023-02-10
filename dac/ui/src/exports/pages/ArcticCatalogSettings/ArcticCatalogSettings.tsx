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

import { useIntl } from "react-intl";
import { Page } from "dremio-ui-lib/dist-esm";
import { ArcticSideNav } from "../../components/SideNav/ArcticSideNav";
import NavCrumbs from "@inject/components/NavCrumbs/NavCrumbs";
import { SimplePageHeader } from "../../components/PageHeaders/SimplePageHeader";

type Props = {
  children: JSX.Element;
};

export const ArcticCatalogSettings = (props: Props) => {
  const { formatMessage } = useIntl();
  return (
    <div className="page-content">
      <ArcticSideNav />
      <Page
        header={
          <>
            <NavCrumbs />
            <SimplePageHeader
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
        {props.children}
      </Page>
    </div>
  );
};

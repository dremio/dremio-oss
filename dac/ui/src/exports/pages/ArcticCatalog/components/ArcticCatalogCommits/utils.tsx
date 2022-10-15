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

import { FormattedMessage } from "react-intl";
// @ts-ignore
import { Button } from "dremio-ui-lib";
import { ArcticCatalogTabsType } from "@app/exports/pages/ArcticCatalog/ArcticCatalog";

import * as classes from "@app/exports/components/ArcticTableHeader/ArcticTableHeader.module.less";

export const getGoToDataButton = (
  handlePush: (tab: ArcticCatalogTabsType) => void
) => {
  return (
    <Button
      color="secondary"
      onClick={() => handlePush("data")}
      disableMargin
      className={classes["arctic-table-header__header-button"]}
    >
      <dremio-icon name="interface/goto-dataset" />
      <FormattedMessage id="ArcticCatalog.Commits.GoToData" />
    </Button>
  );
};

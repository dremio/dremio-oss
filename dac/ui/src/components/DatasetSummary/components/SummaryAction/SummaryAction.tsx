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

import { useMemo } from "react";
import { IconButton } from "dremio-ui-lib/components";
import LinkWithHref from "#oss/components/LinkWithRef/LinkWithRef";
import { VersionContextType } from "dremio-ui-common/components/VersionContext.js";
import { addProjectBase as wrapBackendLink } from "dremio-ui-common/utilities/projectBase.js";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

const { t } = getIntlContext();

type SummaryActionProps = {
  canAlter: boolean;
  canSelect: boolean;
  isView: boolean;
  editLink?: string;
  queryLink?: string;
  resourceId: string;
  versionContext: VersionContextType;
};

function SummaryAction({
  canAlter,
  canSelect,
  isView,
  editLink,
  queryLink,
  resourceId,
  versionContext,
}: SummaryActionProps) {
  const buttonInfo = useMemo(() => {
    const { type, value } = versionContext;
    if (isView && editLink) {
      const editLinkWithRefInfo =
        type && value
          ? editLink + `&refType=${type}&refValue=${value}`
          : editLink;

      if (canAlter) {
        return {
          to: wrapBackendLink(editLinkWithRefInfo),
          tooltip: t("Common.Actions.Edit"),
          icon: "interface/edit",
          alt: "edit",
        };
      } else if (canSelect) {
        return {
          to: wrapBackendLink(editLinkWithRefInfo),
          tooltip: t("Dataset.Actions.GoTo.Dataset"),
          icon: "navigation-bar/go-to-dataset",
          alt: "go to dataset",
        };
      } else if (canAlter === undefined && canSelect === undefined) {
        return {
          to: wrapBackendLink(editLinkWithRefInfo),
          tooltip: t("Common.Actions.Edit"),
          icon: "interface/edit",
          alt: "edit",
        };
      }
    } else if (!isView && queryLink) {
      const queryLinkWithRefInfo =
        type && value && resourceId
          ? queryLink +
            `?refType=${type}&refValue=${value}&sourceName=${resourceId}`
          : queryLink;

      return {
        to: wrapBackendLink(queryLinkWithRefInfo),
        tooltip: t("Dataset.Actions.GoTo.Table"),
        icon: "navigation-bar/go-to-dataset",
        alt: "go to table",
      };
    }
  }, [
    canAlter,
    canSelect,
    isView,
    editLink,
    queryLink,
    resourceId,
    versionContext,
  ]);

  return buttonInfo ? (
    <IconButton
      as={LinkWithHref}
      to={buttonInfo.to}
      tooltipPortal
      tooltip={buttonInfo.tooltip}
    >
      <dremio-icon name={buttonInfo.icon} alt={buttonInfo.alt} />
    </IconButton>
  ) : (
    <></>
  );
}

export default SummaryAction;

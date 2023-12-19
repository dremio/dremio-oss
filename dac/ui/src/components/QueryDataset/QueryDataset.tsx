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

import { IconButton, TooltipPlacement, Button } from "dremio-ui-lib/components";
import { Link } from "react-router";
import { useIntl } from "react-intl";
import { useDispatch } from "react-redux";
// @ts-ignore
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
// @ts-ignore
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { expandExploreSql } from "@app/actions/explore/ui";
import Immutable from "immutable";

type QueryDatasetProps = {
  fullPath: Immutable.Map<any, any>;
  resourceId: string;
  shouldExpandEditor?: boolean;
  className?: string;
  tooltipPlacement?: TooltipPlacement;
  tooltipPortal?: boolean;
  isButton?: boolean;
  buttonClassName?: string;
  versionContext?: { type: string; value: string };
};

// Icon
const QueryDataset = ({
  fullPath,
  className,
  shouldExpandEditor,
  resourceId,
  tooltipPlacement,
  tooltipPortal,
  isButton = false,
  buttonClassName,
  versionContext,
}: QueryDatasetProps) => {
  const dispatch = useDispatch();
  const projectId = getSonarContext()?.getSelectedProjectId?.();
  const newQueryLink = sqlPaths.sqlEditor.link({ projectId });
  const stringifiedFullPath = JSON.stringify(fullPath?.toJS?.());
  const versionedDataset = versionContext
    ? `&versionContext=${JSON.stringify(versionContext)}`
    : "";

  const newQueryUrlParams = `?context="${encodeURIComponent(
    resourceId
  )}"&queryPath=${encodeURIComponent(stringifiedFullPath)}${versionedDataset}`;
  const { formatMessage } = useIntl();
  const handleClick = (e: any) => {
    e.stopPropagation();
    if (shouldExpandEditor) {
      dispatch(expandExploreSql());
    }
  };

  return isButton ? (
    <Button
      tooltip={formatMessage({ id: "Query.Dataset" })}
      variant="secondary"
      as={Link}
      onClick={handleClick}
      to={{
        pathname: newQueryLink,
        search: newQueryUrlParams,
      }}
      className={buttonClassName}
    >
      <dremio-icon class={className} name="navigation-bar/sql-runner" />
      {formatMessage({ id: "Common.DoQuery" })}
    </Button>
  ) : (
    <IconButton
      as={Link}
      onClick={handleClick}
      to={{
        pathname: newQueryLink,
        search: newQueryUrlParams,
      }}
      tooltip={formatMessage({ id: "Query.Dataset" })}
      tooltipPlacement={tooltipPlacement}
      tooltipPortal={tooltipPortal}
    >
      <dremio-icon class={className} name="navigation-bar/sql-runner" />
    </IconButton>
  );
};

export default QueryDataset;

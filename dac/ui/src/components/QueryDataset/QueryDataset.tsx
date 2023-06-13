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

import { IconButton } from "dremio-ui-lib/components";
import { Link } from "react-router";
import { useIntl } from "react-intl";
// @ts-ignore
import * as sqlPaths from "dremio-ui-common/paths/sqlEditor.js";
// @ts-ignore
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";

type QueryDatasetProps = {
  fullPath: string;
  resourceId: string;
  className?: string;
};

const QueryDataset = ({
  fullPath,
  className,
  resourceId,
}: QueryDatasetProps) => {
  const projectId = getSonarContext()?.getSelectedProjectId?.();
  const newQueryLink = sqlPaths.sqlEditor.link({ projectId });
  const newQueryUrlParams = `?context="${encodeURIComponent(
    resourceId
  )}"&queryPath=${fullPath}`;
  const { formatMessage } = useIntl();
  return (
    <IconButton
      as={Link}
      onClick={(e: any) => {
        e.stopPropagation();
      }}
      to={{
        pathname: newQueryLink,
        search: newQueryUrlParams,
      }}
      tooltip={formatMessage({ id: "Query.Dataset" })}
    >
      <dremio-icon class={className} name="navigation-bar/sql-runner" />
    </IconButton>
  );
};

export default QueryDataset;

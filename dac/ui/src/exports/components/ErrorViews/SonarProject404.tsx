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
import { NotFound } from "./NotFound";
import { Link } from "react-router";
import { Button } from "dremio-ui-lib/components";
import * as commonPaths from "dremio-ui-common/paths/common.js";

export const SonarProject404 = () => {
  const { formatMessage } = useIntl();
  return (
    <NotFound
      title={formatMessage({ id: "404.ProjectNotFound" })}
      action={
        <Button
          as={Link}
          variant="primary"
          to={commonPaths.projectsList.link()}
        >
          {formatMessage({ id: "404.GoToSonarProjects" })}
        </Button>
      }
    />
  );
};

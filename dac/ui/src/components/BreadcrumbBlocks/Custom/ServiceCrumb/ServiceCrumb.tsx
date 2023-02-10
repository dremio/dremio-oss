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
import BreadcrumbLink from "../../Common/BreadcrumbLink/BreadcrumbLink";
import * as PATHS from "@app/exports/paths";
//@ts-ignore
import * as commonPaths from "dremio-ui-common/paths/common";
import { withRouter, WithRouterProps } from "react-router";

const ServiceCrumb = (props: WithRouterProps) => {
  const { location } = props;
  const { pathname } = location;
  const isArctic = pathname.indexOf("/arctic") === 0;

  return (
    <BreadcrumbLink
      to={isArctic ? PATHS.arcticCatalogs() : commonPaths.projectsList.link()}
      text={isArctic ? "Arctic (Preview)" : "Sonar"}
      iconName={isArctic ? "corporate/arctic" : "corporate/sonar"}
    />
  );
};

export default withRouter(ServiceCrumb);

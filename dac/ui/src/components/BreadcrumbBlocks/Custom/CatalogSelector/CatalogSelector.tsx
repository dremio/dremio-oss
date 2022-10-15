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
//@ts-ignore
import { getProjects } from "@inject/selectors/projects";
import { withRouter, WithRouterProps } from "react-router";
import { connect } from "react-redux";
import BreadcrumbSelector from "../../Common/BreadcrumbSelector/BreadcrumbSelector";
import Breadcrumb from "../../Common/Breadcrumb/Breadcrumb";
import * as PATHS from "@app/exports/paths";
//@ts-ignore
import { PROJECT_STATES } from "@inject/pages/SettingPage/subpages/projects/ProjectConst";
import { compose } from "redux";
//@ts-ignore
import { useProjectContext } from "@inject/utils/storageUtils/localStorageUtils";
import BreadcrumbLink from "../../Common/BreadcrumbLink/BreadcrumbLink";
import { useArcticCatalogs } from "@app/exports/providers/useArcticCatalogs";

const ARCTIC_PROJECT_TYPE = "DATA_PLANE";
type CatalogSelectorProps = {
  projects: Record<string, any>;
};

const CatalogSelector = (props: CatalogSelectorProps & WithRouterProps) => {
  const {
    projects,
    router,
    location: { pathname },
  } = props;

  const currentProject = useProjectContext();
  const isArctic = pathname.indexOf("/arctic") === 0;
  const splitPath = pathname.split("/").slice(1);
  const [arcticCatalogs] = useArcticCatalogs();

  const changeProject = (newProject: Record<string, any>) => {
    if (isArctic) {
      router.push({
        pathname: PATHS.arcticCatalogDataBase({
          arcticCatalogId: newProject?.id,
        }),
        state: {
          newProject: {},
        },
      });
      return;
    }

    if (currentProject && currentProject.id === newProject?.id) {
      return;
    }

    router.push({
      pathname: "/",
      state: {
        newProject,
      },
    });
  };

  const sonarProjects =
    !isArctic &&
    projects &&
    projects.filter(
      (project: Record<string, any>) => project.type !== ARCTIC_PROJECT_TYPE
    );

  const projectsToShow = isArctic ? arcticCatalogs : sonarProjects;

  const iconName = isArctic
    ? "brand/arctic-catalog-source"
    : "brand/sonar-project";

  const projectOptions =
    Array.isArray(projectsToShow) &&
    projectsToShow
      .filter(
        (project) =>
          project.state === PROJECT_STATES.ACTIVE ||
          project.state === PROJECT_STATES.INACTIVE ||
          project.state === PROJECT_STATES.ACTIVATING
      )
      .map((project: Record<string, any>) => {
        return {
          label: <Breadcrumb text={project.name} iconName={iconName} />,
          onClick: () => changeProject(project),
          value: project.id,
        };
      });

  if (Array.isArray(projectOptions)) {
    return projectOptions.length > 1 ? (
      <BreadcrumbSelector
        defaultValue={isArctic ? splitPath[1] : currentProject?.id}
        options={projectOptions}
      />
    ) : (
      <BreadcrumbLink
        to={
          isArctic && arcticCatalogs
            ? PATHS.arcticCatalogDataBase({
                arcticCatalogId: arcticCatalogs[0]?.id,
              })
            : PATHS.datasets({ projectId: currentProject?.id })
        }
        text={
          isArctic && arcticCatalogs
            ? arcticCatalogs[0]?.name
            : currentProject?.name
        }
        iconName={iconName}
      />
    );
  }
};

const mapStateToProps = (state: Record<string, any>) => {
  return {
    projects: getProjects(state),
  };
};

export default compose(
  withRouter,
  connect(mapStateToProps, {})
)(CatalogSelector);

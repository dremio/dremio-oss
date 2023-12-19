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
import { useEffect, useMemo } from "react";
import { browserHistory } from "react-router";
import BreadcrumbSelector from "../../Common/BreadcrumbSelector/BreadcrumbSelector";
import Breadcrumb from "../../Common/Breadcrumb/Breadcrumb";
import * as PATHS from "@app/exports/paths";
//@ts-ignore
import * as commonPaths from "dremio-ui-common/paths/common";
//@ts-ignore
import PROJECT_STATES from "@inject/constants/projectStates";
//@ts-ignore
import { useProjectContext } from "@inject/utils/storageUtils/localStorageUtils";
import BreadcrumbLink from "../../Common/BreadcrumbLink/BreadcrumbLink";
import { SonarProjectsResource } from "@app/exports/resources/SonarProjectsResource";
import { ArcticCatalogsResource } from "@inject/arctic/resources/ArcticCatalogsResource";
import { useResourceSnapshot } from "smart-resource/react";
import { useDispatch } from "react-redux";
import { resetNessieState } from "@app/actions/nessie/nessie";
import { handleSonarProjectChange } from "@app/utils/projects";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";

const CatalogSelector = () => {
  const dispatch = useDispatch();
  const pathname =
    rmProjectBase(browserHistory.getCurrentLocation().pathname) || "/";
  const currentProject = useProjectContext();
  const sonarProjectId = getSonarContext().getSelectedProjectId!();
  const isArctic = pathname.indexOf("/arctic") === 0;
  const splitPath = pathname.split("/").slice(1);
  const [arcticCatalogs] = useResourceSnapshot(ArcticCatalogsResource);
  const [sonarProjects] = useResourceSnapshot(SonarProjectsResource);

  useEffect(() => {
    if (!isArctic) {
      SonarProjectsResource.fetch();
    } else {
      ArcticCatalogsResource.fetch();
    }
  }, [isArctic]);

  const refetchOnOpen = () => {
    isArctic ? ArcticCatalogsResource.fetch() : SonarProjectsResource.fetch();
  };

  const changeProject = (newProject: Record<string, any>) => {
    if (isArctic) {
      if (splitPath?.[1] && splitPath[1] === newProject?.id) {
        return;
      }
      dispatch(resetNessieState() as any);
      browserHistory.push({
        pathname: PATHS.arcticCatalogDataBase({
          arcticCatalogId: newProject?.id,
        }),
      });
      return;
    }

    if (sonarProjectId === newProject?.id) {
      return;
    }
    handleSonarProjectChange(newProject);
  };

  const getToRoute = (newProject: Record<string, any>) => {
    return commonPaths.projectBase.link({ projectId: newProject.id });
  };

  const iconName = isArctic
    ? "brand/arctic-catalog-source"
    : "brand/sonar-project";

  const projectsToShow: Record<string, any>[] | null = isArctic
    ? arcticCatalogs
    : sonarProjects;

  const projectOptions = useMemo(() => {
    return (
      Array.isArray(projectsToShow) &&
      projectsToShow
        .filter(
          (project: Record<string, any>) =>
            project.state === PROJECT_STATES.ACTIVE ||
            project.state === PROJECT_STATES.INACTIVE ||
            project.state === PROJECT_STATES.DEACTIVATING ||
            project.state === PROJECT_STATES.ACTIVATING
        )
        .map((project: Record<string, any>) => {
          return {
            label: (
              <Breadcrumb
                text={project.name}
                iconName={iconName}
                toRoute={isArctic ? undefined : getToRoute(project)}
              />
            ),
            value: project.id,
            onClick: () => changeProject(project),
          };
        })
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectsToShow]);

  if (Array.isArray(projectOptions)) {
    return projectOptions.length > 1 ? (
      <BreadcrumbSelector
        defaultValue={isArctic ? splitPath[1] : sonarProjectId || ""}
        options={projectOptions}
        refetchOnOpen={refetchOnOpen}
      />
    ) : (
      <BreadcrumbLink
        to={
          isArctic && arcticCatalogs
            ? PATHS.arcticCatalogDataBase({
                arcticCatalogId: arcticCatalogs[0]?.id,
              })
            : commonPaths.projectBase.link({ projectId: sonarProjectId })
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

export default CatalogSelector;

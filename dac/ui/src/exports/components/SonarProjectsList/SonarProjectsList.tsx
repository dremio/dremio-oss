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

import clsx from "clsx";
import { Link } from "react-router";
import { SonarProjectCardSkeleton } from "../SonarProjectCard/SonarProjectCardSkeleton";
import * as PATHS from "../../paths";
import classes from "../ArcticCatalogsList/ArcticCatalogsList.less";
import { SonarProjectCard } from "../SonarProjectCard/SonarProjectCard";
import { setProject } from "./setProject";
type CatalogsListProps = {
  projects: any[] | null;
};

const renderSkeletons = () => Array(6).fill(<SonarProjectCardSkeleton />);

export const SonarProjectsList = (props: CatalogsListProps): JSX.Element => {
  return (
    <ul className={clsx("dremio-layout-grid", classes["catalogs-list"])}>
      {props.projects
        ? props.projects.map((project) => (
            <li key={project.id} className="dremio-link">
              <Link
                to={PATHS.datasets({
                  projectId: project.id,
                })}
                onClick={() => {
                  setProject(project);
                }}
                className="no-underline"
              >
                <SonarProjectCard project={project} />
              </Link>
            </li>
          ))
        : renderSkeletons()}
    </ul>
  );
};

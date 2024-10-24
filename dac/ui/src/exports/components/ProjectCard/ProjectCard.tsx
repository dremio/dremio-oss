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

import { type FunctionComponent } from "react";
import { Avatar, Card } from "dremio-ui-lib/components";
// import { intl } from "#oss/utils/intl";
import type { Project } from "../../types/Project.type";
import classes from "./ProjectCard.less";
import { nameToInitials } from "../../utilities/nameToInitials";

type ProjectCardProps = {
  project: Project;
};

const getIconForProjectType = (type: Project["type"]): JSX.Element => {
  let name = "";
  switch (type) {
    case "ARCTIC":
      name = "brand/arctic-catalog-source";
      break;
    case "SONAR":
      name = "brand/sonar-project";
      break;
  }

  return (
    <dremio-icon
      name={name}
      class={classes["project-card__title-icon"]}
      alt=""
    ></dremio-icon>
  );
};

const getIconForCloudType = (cloud: string) => {
  let name = "";
  switch (cloud) {
    case "AWS":
      name = "corporate/aws";
      break;
  }
  return (
    <dremio-icon
      name={name}
      alt=""
      style={{
        marginRight: "var(--dremio--spacing--05)",
        width: "24px",
        height: "24px",
      }}
    ></dremio-icon>
  );
};

export const ProjectCard: FunctionComponent<ProjectCardProps> = (props) => {
  const { project } = props;
  return (
    <Card
      title={
        <>
          {getIconForProjectType(project.type)}
          <h2>{project.name}</h2>
        </>
      }
    >
      <dl className="dremio-description-list">
        <dt>Created by</dt>
        <dd>
          <Avatar
            initials={nameToInitials(project.createdBy)}
            style={{ marginRight: "var(--dremio--spacing--05)" }}
          />
          {project.createdBy}
        </dd>
        <dt>Cloud</dt>
        <dd>{getIconForCloudType(project.cloud)} Default</dd>
        <dt>Created on</dt>
        <dd>{project.createdOn.toDateString()}</dd>
        {project.type === "SONAR" && (
          <>
            <dt>Engines</dt>
            <dd>{project.engines}</dd>
            <dt>Status</dt>
            <dd>{project.status}</dd>
          </>
        )}
      </dl>
    </Card>
  );
};

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

import { getUsersDetails } from "../Users/getUsersDetails";
import { UserDetails } from "../Users/UserDetails.type";
import { transformSonarProject } from "./transformSonarProject";
import { APIV2Call } from "@app/core/APICall";
import { getApiContext } from "dremio-ui-common/contexts/ApiContext.js";

import VENDORS from "@inject/constants/vendors";

export const listSonarProjectsUrl = () =>
  new APIV2Call().projectScope(false).paths("projects").toString();

export type SonarProject = {
  id: string;
  type: "DATA_PLANE" | "QUERY_ENGINE";
  cloudType: typeof VENDORS.AWS | typeof VENDORS.AZURE;
  createdBy: string;
  createdByDetails?: UserDetails;
  projectStore: string;
  credentials: {
    type:
      | "IAM_ROLE"
      | "IAM_ROLE"
      | "AZURE_STORAGE_CLIENT_CREDENTIALS"
      | "SHARED_ACCESS";
    accessKeyId: string;
    secretAccessKey: null;
  };
};

type ListSonarProjectsParams = {
  filterTypes?: SonarProject["type"][];
  include?: {
    createdByDetails?: boolean;
  };
};

export const listSonarProjects = (
  params: ListSonarProjectsParams
): Promise<SonarProject[]> =>
  getApiContext()
    .fetch(listSonarProjectsUrl())
    .then((res) => res.json())
    .then(async (sonarProjects: SonarProject[]) => {
      let filteredProjects = sonarProjects;

      if (params.filterTypes) {
        filteredProjects = filteredProjects.filter((project) =>
          params.filterTypes?.includes(project.type)
        );
      }

      if (params.include?.createdByDetails === true) {
        const userDetails = await getUsersDetails({
          ids: filteredProjects.map((project) => project.createdBy),
        });
        filteredProjects = filteredProjects.map((project) => {
          return {
            ...project,
            createdByDetails: userDetails.get(project.createdBy),
          };
        });
      }

      return filteredProjects.map(transformSonarProject);
    });

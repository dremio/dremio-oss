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

import {
  Button,
  Page,
  useSegmentedControl,
  Spinner,
} from "dremio-ui-lib/dist-esm";
import NavCrumbs from "@inject/components/NavCrumbs/NavCrumbs";
import { SimplePageHeader } from "../../components/PageHeaders/SimplePageHeader";
import { useSonarProjects } from "../../providers/useSonarProjects";
import { TableViewSwitcher } from "../../components/TableViewSwitcher/TableViewSwitcher";
import { SonarProjectsList } from "../../components/SonarProjectsList/SonarProjectsList";
import { SonarProjectsTable } from "../../components/SonarProjectsTable/SonarProjectsTable";
import { SonarSideNav } from "@app/exports/components/SideNav/SonarSideNav";
import { withRouter } from "react-router";

const SonarProjectsPage = ({ router }: any): JSX.Element => {
  const viewMode = useSegmentedControl<"card" | "grid">("card");
  const [sonarProjects, sonarProjectsStatus] = useSonarProjects();
  return (
    <div className="page-content">
      <SonarSideNav actions={<></>} />
      <Page
        header={
          <>
            <NavCrumbs disableCatalogSelector disableCurrentLocCrumb />
            <SimplePageHeader
              title={
                <span>
                  Sonar Projects
                  {sonarProjectsStatus === "pending" && <Spinner />}
                </span>
              }
              toolbar={
                <>
                  <TableViewSwitcher {...viewMode} />
                  <Button
                    variant="primary"
                    onClick={() =>
                      router.push({
                        pathname: "/setting/projects",
                        state: {
                          modal: "NewProjectModal",
                          projectId: undefined,
                        },
                      })
                    }
                  >
                    <dremio-icon name="interface/add" alt=""></dremio-icon>
                    Add Project
                  </Button>
                </>
              }
            />
          </>
        }
      >
        {sonarProjects?.length === 0 ? (
          <div className="dremio-empty-overlay">No Items</div>
        ) : (
          <div style={{ marginTop: "var(--dremio--spacing--2)" }}>
            {viewMode.value === "card" && (
              <SonarProjectsList projects={sonarProjects} />
            )}
            {viewMode.value === "grid" && (
              <SonarProjectsTable projects={sonarProjects} />
            )}
          </div>
        )}
      </Page>
    </div>
  );
};

export const SonarProjects = withRouter(SonarProjectsPage);

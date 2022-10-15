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

import { createTable } from "leantable/core";
import { useMemo } from "react";
import { projectToRow } from "./projectToRow";
import { columns } from "./columns";

const { render } = createTable({ plugins: [] });

type SonarProjectsTableProps = {
  projects: any | null;
};

const useTableRows = (projects: any) =>
  useMemo(() => {
    if (!projects) {
      return [];
    }
    return projects.map(projectToRow);
  }, [projects]);

export const SonarProjectsTable = (
  props: SonarProjectsTableProps
): JSX.Element => {
  return render({ columns, rows: useTableRows(props.projects) });
};

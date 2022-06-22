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
import { compose } from "redux";
import { connect } from "react-redux";
import { useState, useEffect, useMemo } from "react";
// @ts-ignore
import { Select } from "dremio-ui-lib";

import FieldWithError from "components/Fields/FieldWithError";
import * as ProjectSelectors from "@inject/selectors/projects";
import { useIntl } from "react-intl";

//@ts-ignore
const getNessieProjects = ProjectSelectors?.getNessieProjects || (() => []); //Stub for OSS/Enterprise

type RepoSelectorProps = {
  defaultValue?: string | null;
  onChange?: (value: string) => void;
  nessieProjects: any;
};

const CUSTOM = "customRepo";

const RepoSelector = (props: RepoSelectorProps) => {
  const intl = useIntl();
  const { defaultValue = CUSTOM, onChange, nessieProjects } = props;
  const [project, setProject] = useState(CUSTOM);
  const projectsList = useMemo(
    () => [
      ...nessieProjects.map((item: { name: string; id: string }) => {
        return {
          label: item.name,
          value: item.id,
        };
      }),
      {
        label: intl.formatMessage({ id: "Nessie.CustomRepository" }),
        value: CUSTOM,
      },
    ],
    [nessieProjects, intl]
  );

  const doChange = (value: any) => {
    setProject(value);
    onChange && onChange(value);
  };

  const onRepoChange = (e: { target: { value: any } }) => {
    const value = e.target.value;
    doChange(value);
  };

  //Set initial value on mount
  useEffect(() => {
    if (!defaultValue && projectsList.length > 0) {
      doChange(projectsList[0].value);
    } else {
      const found = projectsList.find((cur) => cur.value === defaultValue);
      if (found) doChange(defaultValue);
      else doChange(CUSTOM);
    }
  }, [projectsList]); //eslint-disable-line

  return (
    <div style={{ marginTop: 6, marginBottom: 12 }}>
      <FieldWithError
        errorPlacement="top"
        label="Nessie Repository"
        labelClass="full-width"
      >
        <div className="full-width">
          <Select
            classes={{ root: "selectRoot__no-padding" }}
            options={projectsList}
            value={project}
            onChange={onRepoChange}
          />
        </div>
      </FieldWithError>
    </div>
  );
};

const mapStateToProps = (state: any) => {
  return {
    //Memoize selector, otherwise will rerender unnecessarily
    nessieProjects: getNessieProjects(state),
  };
};

export default compose(connect(mapStateToProps))(RepoSelector);

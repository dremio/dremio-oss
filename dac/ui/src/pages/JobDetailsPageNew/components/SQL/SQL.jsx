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
import PropTypes from "prop-types";
import { intl } from "#oss/utils/intl";
import CopyButton from "components/Buttons/CopyButton";
import { SqlViewer } from "#oss/exports/components/MonacoWrappers/SqlViewer";

import "./SQL.less";

export const SQL = ({ title, sqlString, sqlClass }) => {
  return (
    <>
      <div className="sql">
        <div className="sql__titleWrapper">
          <span className="sql__title">{title}</span>
          <span className="sql__copyIcon">
            <CopyButton
              data-qa="copy-icon"
              title={intl.formatMessage({ id: "Job.SQL.Copy" })}
              text={sqlString}
            />
          </span>
        </div>
      </div>
      <div className={sqlClass}>
        <SqlViewer style={{ height: 190 }} value={sqlString} />
      </div>
    </>
  );
};

SQL.propTypes = {
  title: PropTypes.string,
  sqlClass: PropTypes.string,
  sqlString: PropTypes.string,
};

export default SQL;

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
import { useState } from "react";
import PropTypes from "prop-types";
import { Tooltip } from "dremio-ui-lib";
import { intl } from "@app/utils/intl";
import CopyButton from "components/Buttons/CopyButton";
import SqlEditor from "@app/components/SQLEditor.js";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import { SQL_DARK_THEME, SQL_LIGHT_THEME } from "@app/utils/sql-editor";

import "./SQL.less";

const options = {
  selectOnLineNumbers: false,
  disableLayerHinting: true,
  wordWrap: "on",
  overviewRulerBorder: false,
  lineNumbers: "on",
  readOnly: true,
  minimap: {
    enabled: false,
  },
};
export const SQL = ({
  title,
  sqlString,
  onClick,
  showContrast,
  customOptions = {},
  sqlClass,
  defaultContrast,
}) => {
  const [isContrast, setIsContrast] = useState(defaultContrast);
  const theme = isContrast ? SQL_DARK_THEME : SQL_LIGHT_THEME;
  const background = isContrast ? "#333333" : "#F3F4F4";
  const selectionBackground = isContrast ? "#304D6D" : "#B5D5FB";
  const inactiveSelectionBackground = isContrast ? "#505862" : "#c6e9ef";

  const handleClick = () => {
    localStorageUtils.setSqlThemeContrast(!isContrast);
    setIsContrast(!isContrast);
    if (onClick && typeof onClick === "function") {
      onClick(!isContrast);
    }
  };
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
        {showContrast && (
          <span
            data-qa="toggle-icon"
            id="toggle-icon"
            className="sql__toggleIcon"
            onClick={handleClick}
          >
            <Tooltip
              title={isContrast ? "Common.Theme.Dark" : "Common.Theme.Light"}
            >
              <dremio-icon
                name="sql-editor/sqlThemeSwitcher"
                alt="Theme Switcher"
                class="theme-switcher-icon"
              />
            </Tooltip>
          </span>
        )}
      </div>
      <div className={sqlClass}>
        <SqlEditor
          height={190}
          readOnly
          value={sqlString}
          fitHeightToContent={false}
          maxHeight={190}
          contextMenu={false}
          customTheme
          theme={theme}
          background={background}
          selectionBackground={selectionBackground}
          inactiveSelectionBackground={inactiveSelectionBackground}
          customOptions={{ ...options, ...customOptions }}
        />
      </div>
    </>
  );
};

SQL.propTypes = {
  title: PropTypes.string,
  contrast: PropTypes.bool,
  showContrast: PropTypes.bool,
  sqlClass: PropTypes.string,
  customOptions: PropTypes.object,
  sqlString: PropTypes.string,
  isContrast: PropTypes.bool,
  defaultContrast: PropTypes.bool,
  onClick: PropTypes.func,
};
SQL.defaultProps = {
  defaultContrast: true,
};
export default SQL;

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
import { injectIntl } from "react-intl";
import PropTypes from "prop-types";
import classNames from "classnames";
import Art from "@app/components/Art";
import "./TopPanel.less";
import { getTagClassName } from "dyn-load/utils/jobsUtils";

const renderIcon = (iconName, className, selected) => {
  return (
    <Art
      src={iconName}
      alt="icon"
      title="icon"
      className={classNames("topPanel__icon", className, {
        "--selected": selected,
      })}
    />
  );
};

export const TopPanelTab = (props) => {
  const {
    intl: { formatMessage },
    tabName,
    onTabClick,
    selectedTab,
    iconName,
  } = props;

  return (
    <div
      onClick={() => onTabClick(tabName)}
      className={
        selectedTab === tabName
          ? "topPanel__tab --selected"
          : "topPanel__tab --unselected"
      }
    >
      {renderIcon(iconName, getTagClassName(tabName), selectedTab === tabName)}
      {formatMessage({ id: `TopPanel.${tabName}` })}
    </div>
  );
};

TopPanelTab.propTypes = {
  intl: PropTypes.object.isRequired,
  tabName: PropTypes.string.isRequired,
  onTabClick: PropTypes.func.isRequired,
  selectedTab: PropTypes.string,
  iconName: PropTypes.string.isRequired,
};

export default injectIntl(TopPanelTab);

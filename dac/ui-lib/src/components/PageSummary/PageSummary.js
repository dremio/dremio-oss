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
import React, { useState } from "react";
import PropTypes from "prop-types";

import { ReactComponent as SummaryIcon } from "../../art/Summary.svg";
import { ReactComponent as ArrowUpIcon } from "../../art/ArrowUp.svg";
import { ReactComponent as ArrowDownIcon } from "../../art/ArrowDown.svg";

import "./pageSummary.scss";

const PageSummary = (props) => {
  const [isCollapsed, setIsCollapsed] = useState(false);

  const {
    classes: { root, header, body },
    data,
    title,
  } = props;

  const toggleSummary = () => setIsCollapsed(!isCollapsed);

  return (
    <div className={`summary gutter ${root}`}>
      <div className={`summary__header ${header}`}>
        <SummaryIcon className="summary__header__icon margin-right" />
        <h3 className="summary__header__title">{title}</h3>
        {isCollapsed ? (
          <ArrowUpIcon
            className="summary__header__icon"
            onClick={toggleSummary}
          />
        ) : (
          <ArrowDownIcon
            className="summary__header__icon"
            onClick={toggleSummary}
          />
        )}
      </div>
      {!isCollapsed && (
        <div className={`summary__body gutter-top ${body}`}>
          {data &&
            data.map(({ label, value }) => (
              <div className="item gutter" key={label}>
                <span className="item__label margin-bottom--half">{label}</span>
                <span className="item__value">{value}</span>
              </div>
            ))}
        </div>
      )}
    </div>
  );
};

PageSummary.propTypes = {
  data: PropTypes.arrayOf(
    PropTypes.shape({
      label: PropTypes.oneOfType([PropTypes.string, PropTypes.node]),
      value: PropTypes.oneOfType([PropTypes.string, PropTypes.node]),
    })
  ),
  title: PropTypes.string,
  classes: PropTypes.shape({
    root: PropTypes.string,
    header: PropTypes.string,
    body: PropTypes.string,
  }),
};

export default PageSummary;

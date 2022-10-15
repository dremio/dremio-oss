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
import { injectIntl } from "react-intl";
import Immutable from "immutable";
import jobsUtils from "@app/utils/jobsUtils";
import { Tooltip } from "dremio-ui-lib";
import { getIconPath } from "@app/utils/getIconPath";
import timeUtils from "utils/timeUtils";

import "./Reflection.less";

const renderIcon = (iconName, className) => {
  return (
    <Tooltip title="Reflection">
      <img src={getIconPath(iconName)} alt="Reflection" className={className} />
    </Tooltip>
  );
};

const getReflectionIcon = (reflectionType) => {
  const reflectionCreatedIcon =
    reflectionType === "RAW"
      ? "interface/reflections-created-raw"
      : "interface/reflections-created-agg";
  return renderIcon(reflectionCreatedIcon, "reflectionIcon");
};

const ReflectionsCreated = (props) => {
  const {
    reflections,
    intl: { formatMessage },
    location,
  } = props;

  return (
    <div>
      {reflections.size > 0 && (
        <div className="reflection-header">
          <div className="reflection-header__title">
            {formatMessage({ id: "Reflections.ReflectionCreated" })}
          </div>
        </div>
      )}
      {reflections &&
        reflections.map((item, index) => {
          const reflectionAge =
            item.get("reflectionCreated") && item.get("reflectionCreated");
          return (
            <div
              className="jobsDetailsReflections"
              key={`reflections-${index}`}
              data-qa="reflectionsTestCase"
            >
              <span className="jobsDetailsReflections__contentWrapper">
                {getReflectionIcon(item.get("reflectionType"))}
                <span className="jobsDetailsReflections__contentWrapper">
                  <div className="jobsDetailsReflections__contentWrapper__header">
                    {jobsUtils.getReflectionsLink(item, location)}
                  </div>
                  <div className="jobsDetailsReflections__contentWrapper__path">
                    {item.get("reflectionDatasetPath")}
                  </div>
                </span>
              </span>
              {reflectionAge && (
                <span className="jobsDetailsReflections__age">
                  {formatMessage({ id: "Reflections.Age" })}
                  {timeUtils.toNow(Number(reflectionAge))}
                </span>
              )}
            </div>
          );
        })}
    </div>
  );
};

ReflectionsCreated.propTypes = {
  intl: PropTypes.object.isRequired,
  isAcceleration: PropTypes.bool,
  reflections: PropTypes.instanceOf(Immutable.List),
  location: PropTypes.object,
};
export default injectIntl(ReflectionsCreated);

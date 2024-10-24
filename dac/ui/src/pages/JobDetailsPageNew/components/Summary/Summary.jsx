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
import { Tooltip } from "dremio-ui-lib";
import EllipsedText from "#oss/components/EllipsedText";
import "./Summary.less";

const Summary = ({ jobSummary, intl: { formatMessage } }) => {
  return (
    <>
      <div className="summary__title">{formatMessage({ id: "Summary" })}</div>
      <div>
        {jobSummary.map((item, index) => {
          return (
            <>
              <div key={`jobSummary-${index}`} className="summary__content">
                <span className="summary__contentHeader">
                  {formatMessage({ id: item.label })}:
                </span>
                {item.label !== "Common.User" ? (
                  <span className="summary__contentValue">{item.content}</span>
                ) : item.content?.length > 24 ? (
                  <Tooltip title={item.content}>
                    <EllipsedText
                      text={item.content}
                      className=" summary__contentValue ellipseLabel"
                    />
                  </Tooltip>
                ) : (
                  <span className="summary__contentValue">{item.content}</span>
                )}
              </div>
              {item?.secondaryContent ? item.secondaryContent : null}
            </>
          );
        })}
      </div>
    </>
  );
};
Summary.propTypes = {
  intl: PropTypes.object.isRequired,
  jobSummary: PropTypes.array,
};
export default injectIntl(Summary);

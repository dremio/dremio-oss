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
import Immutable from "immutable";
import { useRef, useState } from "react";
// @ts-ignore
import { Tooltip } from "dremio-ui-lib";
import { addTooltip } from "../../datasetSummaryUtils";
import VersionContext, {
  VersionContextType,
} from "dremio-ui-common/components/VersionContext.js";

import * as classes from "./SummarySubHeader.module.less";

const SummarySubHeader = ({
  subTitle,
  versionContext,
  detailsView,
}: {
  subTitle: string;
  versionContext?: VersionContextType;
  detailsView?: boolean;
}) => {
  const [showTooltip, setShowTooltip] = useState(false);
  const subTitleRef = useRef(null);

  return (
    <div
      className={`${classes["summary-subHeader-container"]} ${
        detailsView ? classes["summary-subHeader-container__details"] : ""
      }`}
    >
      {showTooltip ? (
        <Tooltip interactive title={subTitle}>
          <p className={classes["summary-subHeader"]}>{subTitle}</p>
        </Tooltip>
      ) : (
        <p
          ref={subTitleRef}
          onMouseEnter={() => addTooltip(subTitleRef, setShowTooltip)}
          className={classes["summary-subHeader"]}
        >
          {subTitle}
        </p>
      )}
      {versionContext && (
        <VersionContext
          versionContext={versionContext}
          className={classes["summary-secondarySubHeader"]}
          withRefKeyword
        />
      )}
    </div>
  );
};

export default SummarySubHeader;

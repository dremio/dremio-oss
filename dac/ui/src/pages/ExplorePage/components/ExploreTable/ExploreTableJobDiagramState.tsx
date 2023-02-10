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

import { FormattedMessage } from "react-intl";
import LottieImages from "@app/components/LottieImages";
import * as classes from "./ExploreTableJobDiagram.module.less";

type ExploreTableJobDiagramStateProps = {
  isCurrentState: boolean;
  isStateComplete: boolean;
  state: string;
};

function ExploreTableJobDiagramState({
  isCurrentState,
  isStateComplete,
  state,
}: ExploreTableJobDiagramStateProps) {
  return (
    <div className={classes["sqlEditor__jobDiagram__state"]}>
      {isStateComplete && (
        <dremio-icon
          name="engine-state/running"
          class={classes["stateIcon--success"]}
        />
      )}
      {isCurrentState && (
        <LottieImages
          imageWidth={20}
          imageHeight={20}
          src="generic_loader.json"
        />
      )}
      {!isStateComplete && !isCurrentState && (
        <dremio-icon name="interface/circle" class={classes["stateIcon"]} />
      )}
      <span
        className={
          !isStateComplete && !isCurrentState
            ? classes["stateName--pending"]
            : classes["stateName"]
        }
      >
        <FormattedMessage id={state} />
      </span>
    </div>
  );
}

export default ExploreTableJobDiagramState;

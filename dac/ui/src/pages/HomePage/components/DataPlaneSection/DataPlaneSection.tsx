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
import { useIntl } from "react-intl";
// @ts-ignore
import { isDataPlaneEnabled } from "@inject/utils/dataPlaneUtils";
import FinderNav from "@app/components/FinderNav";
import ViewStateWrapper from "@app/components/ViewStateWrapper";
import SourceBranchPicker from "../SourceBranchPicker/SourceBranchPicker";
import { spacesSourcesListSpinnerStyleFinderNav } from "@app/pages/HomePage/HomePageConstants";

type DataPlaneSectionProps = {
  dataPlaneSources: any;
  sourcesViewState: any;
  addHref: () => void;
  height?: string;
  location?: any;
  onToggle?: any;
  isCollapsed?: boolean;
  isCollapsible?: boolean;
};

function DataPlaneSection({
  dataPlaneSources,
  sourcesViewState,
  addHref,
  height,
  location,
  onToggle = null,
  isCollapsed = false,
  isCollapsible = false,
}: DataPlaneSectionProps) {
  const intl = useIntl();
  if (!isDataPlaneEnabled) return null;

  return (
    <div
      className="left-tree-wrap"
      style={{
        height: dataPlaneSources.size ? height : "auto",
        overflow: "hidden",
      }}
    >
      <ViewStateWrapper
        viewState={sourcesViewState}
        spinnerStyle={spacesSourcesListSpinnerStyleFinderNav}
      >
        <FinderNav
          isCollapsed={isCollapsed}
          isCollapsible={isCollapsible}
          onToggle={onToggle}
          location={location}
          navItems={dataPlaneSources}
          title={intl.formatMessage({ id: "Source.DataPlanes" })}
          addTooltip={intl.formatMessage({ id: "Source.AddDataPlane" })}
          isInProgress={sourcesViewState.get("isInProgress")}
          addHref={addHref}
          listHref="/sources/dataplane/list"
          renderExtra={(item: any, targetRef: any) => (
            <SourceBranchPicker source={item} anchorEl={targetRef.current} />
          )}
        />
      </ViewStateWrapper>
    </div>
  );
}
export default DataPlaneSection;

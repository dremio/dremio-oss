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
import clsx from "clsx";
import { connect, useSelector } from "react-redux";
import Immutable from "immutable";
import { useIntl } from "react-intl";
import { WithRouterProps, withRouter } from "react-router";
import { compose } from "redux";
import { orderBy } from "lodash";
import FinderNav from "@app/components/FinderNav";
import { getViewState } from "@app/selectors/resources";
import {
  getHomeContents,
  getHomeSource,
  getSortedSources,
} from "@app/selectors/home";

import { getAdminStatus } from "dyn-load/pages/HomePage/components/modals/SpaceModalMixin";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { useSonarContents } from "@app/exports/providers/useSonarContents";
import FinderNavItem from "@app/components/FinderNavItem";
import SourceBranchPicker from "./SourceBranchPicker/SourceBranchPicker";
import { ENTITY_TYPES } from "@app/constants/Constants";
import { decorateFolder } from "@app/utils/decorators/resourceDecorators";
import { useMemo } from "react";
import { getRefQueryParams } from "@app/utils/nessieUtils";
import LinkWithRef from "@app/components/LinkWithRef/LinkWithRef";
import { IconButton } from "dremio-ui-lib/components";
import { SHOW_ADD_FOLDER } from "@inject/pages/HomePage/components/HeaderButtonConstants";

import * as classes from "./HomeSourceSection.module.less";

type HomeSourceSectionProps = {
  homeSource: any;
  sourcesViewState: any;
  homeSourceUrl: string;
  isAdmin: boolean;
};

function HomeSourceSection(props: HomeSourceSectionProps & WithRouterProps) {
  const { homeSource, sourcesViewState, homeSourceUrl } = props;

  const nessie = useSelector((state: any) => state.nessie);
  const [result, , status] = useSonarContents({
    path: rmProjectBase(homeSourceUrl),
    entityType: ENTITY_TYPES.source,
    ...getRefQueryParams(nessie, homeSource.name),
  });

  const decoratedFolders = useMemo(() => {
    return Immutable.fromJS(
      orderBy(
        result?.contents?.folders?.map((folder: any) =>
          // Adds entityType to folder data for rendering icon
          decorateFolder(Immutable.fromJS(folder)).toJS(),
        ) || [],
        [(folder) => folder.name.toLowerCase()],
      ),
    );
  }, [result]);

  const viewState = Immutable.fromJS({
    isInProgress: status === "pending" || sourcesViewState.get("isInProgress"),
  });
  const showAdd = status === "success";
  const { formatMessage } = useIntl();

  return (
    <div className={clsx("left-tree-wrap", classes["home-source-section"])}>
      <FinderNav
        title=""
        noMarginTop
        navItems={decoratedFolders}
        isInProgress={viewState.get("isInProgress")}
        renderLink={() =>
          homeSource ? (
            <>
              <FinderNavItem
                isLoading={viewState.get("isInProgress")}
                linkClass={classes["homeSourceNavItem"]}
                item={homeSource}
                onlyActiveOnIndex={true}
                renderExtra={(item: any, targetRef: any) => (
                  <>
                    <SourceBranchPicker
                      source={item}
                      getAnchorEl={() => targetRef.current}
                    />
                  </>
                )}
              />
              {SHOW_ADD_FOLDER && showAdd && (
                <IconButton
                  className="add-button"
                  tooltip={formatMessage({ id: "Common.NewFolder" })}
                  as={LinkWithRef}
                  to={{
                    ...props.location,
                    state: {
                      modal: "AddFolderModal",
                      rootPath: homeSource.resourcePath,
                      rootName: homeSource.name,
                    },
                  }}
                >
                  <dremio-icon
                    name="interface/add-small"
                    class="add-space-icon"
                  />
                </IconButton>
              )}
            </>
          ) : null
        }
      />
    </div>
  );
}

const mapStateToProps = (state: any) => {
  const sources = getSortedSources(state);
  const homeSource = getHomeSource(sources);

  return {
    homeSource: homeSource ? homeSource.toJS() : null,
    sourcesViewState: getViewState(state, "AllSources"),
    isAdmin: getAdminStatus(state),
    entity: getHomeContents(state),
  };
};

export default compose(connect(mapStateToProps), withRouter)(HomeSourceSection);

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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import { withRouter } from "react-router";
import { connect } from "react-redux";
import Immutable from "immutable";
import { injectIntl } from "react-intl";
import { CombinedActionMenuMixin } from "dyn-load/components/Menus/ExplorePage/CombinedActionMenuMixin";
import { getAllJobDetails } from "@app/selectors/exploreJobs";
import { HoverHelp } from "dremio-ui-lib";

import Menu from "./Menu";
import MenuLabel from "./MenuLabel";

@injectIntl
@CombinedActionMenuMixin
class CombinedActionMenu extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    datasetColumns: PropTypes.array,
    closeMenu: PropTypes.func,
    isSettingsDisabled: PropTypes.bool,
    updateDownloading: PropTypes.func,
    intl: PropTypes.object.isRequired,
    //connected
    jobDetails: PropTypes.object,
  };

  static contextTypes = {
    router: PropTypes.object,
    location: PropTypes.object,
  };

  renderDownloadSectionHeader = () => {
    const { intl, jobDetails } = this.props;
    const isRun = jobDetails?.queryType === "UI_RUN";
    const headerText = isRun
      ? jobDetails.isOutputLimited
        ? intl.formatMessage({ id: "Explore.Download.Limited" })
        : "Download"
      : intl.formatMessage({ id: "Explore.Download.Sample" });
    const helpContent = isRun
      ? jobDetails.isOutputLimited
        ? intl.formatMessage(
            { id: "Explore.Run.Warning" },
            { rows: jobDetails.outputRecords },
          )
        : ""
      : intl.formatMessage({ id: "Explore.Preview.Warning" });
    return (
      <MenuLabel>
        {headerText}
        {!!helpContent && (
          <HoverHelp content={helpContent} placement="bottom-start" />
        )}
      </MenuLabel>
    );
  };

  render() {
    return <Menu>{this.checkToRenderDownloadSection()}</Menu>;
  }
}

function mapStateToProps(state, ownProps) {
  const { dataset } = ownProps;

  return {
    jobDetails: getAllJobDetails(state)[dataset.get("jobId")],
  };
}

export default withRouter(connect(mapStateToProps)(CombinedActionMenu));

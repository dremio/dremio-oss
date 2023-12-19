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
import { CombinedActionMenuMixin } from "dyn-load/components/Menus/ExplorePage/CombinedActionMenuMixin.js";
import { getJobProgress } from "@app/selectors/explore";
import { HoverHelp } from "dremio-ui-lib";

import Menu from "./Menu";
import MenuLabel from "./MenuLabel";

@injectIntl
@CombinedActionMenuMixin
export class CombinedActionMenu extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map),
    datasetColumns: PropTypes.array,
    closeMenu: PropTypes.func,
    isSettingsDisabled: PropTypes.bool,
    updateDownloading: PropTypes.func,
    intl: PropTypes.object.isRequired,
    //connected
    jobProgress: PropTypes.object,
  };

  static contextTypes = {
    router: PropTypes.object,
    location: PropTypes.object,
  };

  renderDownloadSectionHeader = () => {
    const { intl, jobProgress } = this.props;
    const isRun = jobProgress && jobProgress.isRun;
    const headerText = isRun
      ? intl.formatMessage({ id: "Explore.Download.Limited" })
      : intl.formatMessage({ id: "Explore.Download.Sample" });
    const helpContent = isRun
      ? intl.formatMessage({ id: "Explore.Run.Warning" })
      : intl.formatMessage({ id: "Explore.Preview.Warning" });
    return (
      <MenuLabel>
        {headerText}
        <HoverHelp content={helpContent} placement="bottom-start" />
      </MenuLabel>
    );
  };

  render() {
    return <Menu>{this.checkToRenderDownloadSection()}</Menu>;
  }
}

function mapStateToProps(state) {
  return {
    jobProgress: getJobProgress(state),
  };
}

export default withRouter(connect(mapStateToProps)(CombinedActionMenu));

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
import { compose } from "redux";
import { connect } from "react-redux";
import authorize from "@inject/containers/authorize";
import settingActions, { getDefinedSettings } from "actions/resources/setting";
import { getViewState } from "selectors/resources";
import Immutable from "immutable";
import SettingHeader from "@app/components/SettingHeader";
import ViewStateWrapper from "@app/components/ViewStateWrapper";
import { RESERVED as SUPPORT_ACCESS_RESERVED } from "@inject/pages/AdminPage/subpages/SupportAccess";
import FormUnsavedRouteLeave from "@app/components/Forms/FormUnsavedRouteLeave";
import BiApplicationTools, {
  RESERVED as BIAPPLICATION_TOOLS_RESERVED,
} from "@app/pages/AdminPage/subpages/BiApplicationTools";

import SettingsMicroForm from "./SettingsMicroForm";
import { LABELS_IN_SECTIONS } from "./settingsConfig";
import { RESERVED as INTERNAL_SUPPORT_RESERVED } from "./InternalSupportEmail";

import "./BiApplications.less";

export const VIEW_ID = "SUPPORT_SETTINGS_VIEW_ID";

export const RESERVED = new Set([
  ...(SUPPORT_ACCESS_RESERVED || []),
  ...INTERNAL_SUPPORT_RESERVED,
  ...BIAPPLICATION_TOOLS_RESERVED,
]);

export class BiApplications extends PureComponent {
  static propTypes = {
    getDefinedSettings: PropTypes.func.isRequired,
    viewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    settings: PropTypes.instanceOf(Immutable.Map).isRequired,
    setChildDirtyState: PropTypes.func,
    getSetting: PropTypes.func,
  };

  componentWillMount() {
    this.props.getDefinedSettings(
      [...RESERVED, ...Object.keys(LABELS_IN_SECTIONS)],
      true,
      VIEW_ID
    );
  }

  state = {
    getSettingInProgress: false,
    tempShown: new Immutable.OrderedSet(),
  };

  renderSettingsMicroForm = (settingId, props) => {
    const formKey = "settings-" + settingId;
    return (
      <SettingsMicroForm
        updateFormDirtyState={this.props.setChildDirtyState(formKey)}
        form={formKey}
        key={formKey}
        settingId={settingId}
        viewId={VIEW_ID}
        style={{ margin: "5px 0" }}
        {...props}
      />
    );
  };

  render() {
    const viewStateWithoutError = this.props.viewState.set("isFailed", false);
    return (
      <div className="biApplication-settings">
        <SettingHeader icon="Chart.svg">{la("BI Applications")}</SettingHeader>
        <ViewStateWrapper
          viewState={viewStateWithoutError}
          hideChildrenWhenFailed={false}
          style={{ overflow: "auto", height: "100%", flex: "1 1 auto" }}
        >
          <BiApplicationTools
            renderSettings={this.renderSettingsMicroForm}
            settings={this.props.settings}
          />
        </ViewStateWrapper>
      </div>
    );
  }
}

function mapStateToProps(state) {
  return {
    viewState: getViewState(state, VIEW_ID),
    settings: state.resources.entities.get("setting"),
  };
}

export default compose(
  authorize("Support"),
  connect(mapStateToProps, {
    // todo: find way to auto-inject PropTypes for actions
    getSetting: settingActions.get.dispatch,
    getDefinedSettings,
  }),
  FormUnsavedRouteLeave
)(BiApplications);

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
import { connect } from "react-redux";
import Immutable from "immutable";

import { getDefinedSettings } from "actions/resources/setting";
import { getViewState } from "selectors/resources";

import ViewStateWrapper from "components/ViewStateWrapper";
import FormUnsavedRouteLeave from "components/Forms/FormUnsavedRouteLeave";
import SettingHeader from "@app/components/SettingHeader";

import SettingsMicroForm from "./SettingsMicroForm";
import { LABELS, SECTIONS } from "./settingsConfig";

export const VIEW_ID = "ADVANCED_SETTINGS_VIEW_ID";

export class Advanced extends PureComponent {
  static propTypes = {
    getAllSettings: PropTypes.func.isRequired,
    setChildDirtyState: PropTypes.func,
    viewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    settings: PropTypes.object,
  };

  componentWillMount() {
    this.props.getAllSettings(Object.keys(LABELS), false, VIEW_ID); // all settings asumed to be loaded and used inside SettingsMicroForm
  }

  renderMicroForm(settingId, display) {
    const formKey = "settings-" + settingId;
    let show = true;
    if (!settingId.includes("enable")) {
      if (display === false) show = false;
      else show = true;
    }
    if (show)
      return (
        <SettingsMicroForm
          updateFormDirtyState={this.props.setChildDirtyState(formKey)}
          style={{
            marginTop:
              settingId.includes("enable") || settingId.includes("threshold")
                ? 26
                : 10,
            display: "block",
          }}
          form={formKey}
          key={formKey}
          settingId={settingId}
          viewId={VIEW_ID}
          from="queueControl"
        />
      );
  }

  renderSectionIfEnabled(name, items) {
    let display = false;
    if (this.props.settings)
      return (
        <div
          key={name}
          style={{
            padding: "20px 0 20px",
            borderBottom: "1px solid hsla(0, 0%, 0%, 0.1)",
          }}
        >
          <span style={{ fontSize: "18px", fontWeight: 600 }}>{name}</span>
          {Array.from(items).map(([settingId]) => {
            if (settingId.includes("enable")) {
              const currentValue = this.props.settings;
              if (currentValue.size > 0) {
                const enableFlag = currentValue.get(settingId)
                  ? currentValue.get(settingId).get("value")
                  : true;
                if (enableFlag || enableFlag === undefined) {
                  display = true;
                }
              }
            } else if (settingId.includes("threshold")) {
              display = true;
            }
            return this.renderMicroForm(settingId, display);
          })}
        </div>
      );
  }

  renderSections() {
    return Array.from(SECTIONS).map(([name, items]) => {
      return this.renderSectionIfEnabled(name, items);
    });
  }

  render() {
    // SettingsMicroForm has a logic for error display. We should not duplicate it in the viewState
    const viewStateWithoutError = this.props.viewState.set("isFailed", false);
    return (
      <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
        <SettingHeader icon="settings/queue-control">
          {la("Queue Control")}
        </SettingHeader>
        <ViewStateWrapper
          viewState={viewStateWithoutError}
          style={{ overflow: "auto", height: "100%", flex: "1 1 auto" }}
          hideChildrenWhenFailed={false}
        >
          {this.renderSections()}
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

export default connect(mapStateToProps, {
  getAllSettings: getDefinedSettings,
})(FormUnsavedRouteLeave(Advanced));

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

import { useState, useEffect } from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import Immutable from "immutable";
import SettingHeader from "@app/components/SettingHeader";
import ViewStateWrapper from "@app/components/ViewStateWrapper";
import Toggle from "@app/components/Fields/Toggle";
import { fetchSupportFlags, saveSupportFlag } from "@app/actions/supportFlags";
import { FormattedMessage } from "react-intl";
import { getViewState } from "selectors/resources";
import { getSupportFlags } from "@app/selectors/supportFlags";
import authorize from "@inject/containers/authorize";
import config from "@inject/utils/config";
import { isEnterprise, isCommunity } from "dyn-load/utils/versionUtils";
import { compose } from "redux";

import "./SqlEditor.less";

export const VIEW_ID = "SUPPORT_SETTINGS_VIEW_ID";

const SqlEditor = (props) => {
  const {
    dispatchFetchSupportFlags,
    viewState,
    dispatchSaveSupportFlag,
    supportFlags,
  } = props;
  const viewStateWithoutError = viewState.set("isFailed", false);
  const isAutoCompleteEnabled =
    (supportFlags && supportFlags["ui.autocomplete.allow"]) ||
    config.autoComplete;
  const [isEnabled, setIsEnabled] = useState(isAutoCompleteEnabled);

  useEffect(() => {
    dispatchFetchSupportFlags("ui.autocomplete.allow");
  }, [dispatchFetchSupportFlags]);

  useEffect(() => {
    const isEnterpriseFlag = isEnterprise && isEnterprise();
    const isCommunityFlag = isCommunity && isCommunity();

    if (isEnterpriseFlag || isCommunityFlag) {
      return;
    }
    if (supportFlags && supportFlags["ui.autocomplete.allow"] !== undefined) {
      setIsEnabled(supportFlags["ui.autocomplete.allow"]);
    }
  }, [supportFlags]);

  const handleChange = () => {
    const saveObj = {
      type: "BOOLEAN",
      id: "ui.autocomplete.allow",
      value: !isEnabled,
    };
    dispatchSaveSupportFlag("ui.autocomplete.allow", saveObj);
    setIsEnabled(!isEnabled);
  };

  return (
    <div className="autocomplete-settings-main">
      <SettingHeader icon="common/SQLRunner">
        <FormattedMessage id="Admin.SqlEditor.title" />
      </SettingHeader>
      <div className="gutter-left--double">
        <ViewStateWrapper
          viewState={viewStateWithoutError}
          hideChildrenWhenFailed={false}
          style={{ overflow: "auto", height: "100%", flex: "1 1 auto" }}
        >
          <div className="enable-autcomplete-div">
            <span>
              <FormattedMessage id="Admin.SqlEditor.subtitle" />
            </span>
          </div>
          <div className="enable-autocomplete-subtext">
            <span>
              <FormattedMessage id="Admin.SqlEditor.subtext" />
            </span>
          </div>
          <div className="autocomplete-button-section">
            <div>
              <span>
                <FormattedMessage id="Admin.SqlEditor.actionName" />
              </span>
            </div>
            <div className="autocomplete-toggle-div">
              <Toggle value={isEnabled} onChange={handleChange} />
            </div>
          </div>
          <hr className="setting-body-sql-hr" />
        </ViewStateWrapper>
      </div>
    </div>
  );
};

const mapStateToProps = (state) => {
  return {
    viewState: getViewState(state, VIEW_ID),
    supportFlags: getSupportFlags(state),
  };
};

const mapDispatchToProps = {
  dispatchFetchSupportFlags: fetchSupportFlags,
  dispatchSaveSupportFlag: saveSupportFlag,
};

SqlEditor.propTypes = {
  dispatchFetchSupportFlags: PropTypes.func,
  dispatchSaveSupportFlag: PropTypes.func,
  viewState: PropTypes.instanceOf(Immutable.Map).isRequired,
  supportFlags: PropTypes.object,
};

export default compose(
  authorize("SqlEditor"),
  connect(mapStateToProps, mapDispatchToProps)
)(SqlEditor);

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
import { Component } from "react";
import PropTypes from "prop-types";
import { FormattedMessage } from "react-intl";

import MenuItem from "components/Menus/MenuItem";
import { HANDLE_THROUGH_API } from "@inject/pages/HomePage/components/HeaderButtonConstants";

export default class AnalyzeMenuItems extends Component {
  static propTypes = {
    openTableau: PropTypes.func,
    openQlikSense: PropTypes.func,
    openPowerBI: PropTypes.func,
    analyzeToolsConfig: PropTypes.object,
  };

  render() {
    const { analyzeToolsConfig } = this.props;

    let showTableau = analyzeToolsConfig.tableau.enabled;
    let showPowerBI = analyzeToolsConfig.powerbi.enabled;

    if (HANDLE_THROUGH_API) {
      const supportFlags = localStorage.getItem("supportFlags")
        ? JSON.parse(localStorage.getItem("supportFlags"))
        : null;

      showTableau = supportFlags && supportFlags["client.tools.tableau"];
      showPowerBI = supportFlags && supportFlags["client.tools.powerbi"];
    }

    return (
      <div>
        {showTableau && (
          <MenuItem onClick={this.props.openTableau}>
            <FormattedMessage id="Dataset.Tableau" />
          </MenuItem>
        )}
        {showPowerBI && (
          <MenuItem onClick={this.props.openPowerBI}>
            <FormattedMessage id="Dataset.PowerBI" />
          </MenuItem>
        )}
        {analyzeToolsConfig.qlik.enabled && (
          <MenuItem onClick={this.props.openQlikSense}>
            <FormattedMessage id="Dataset.QlikSense" />
          </MenuItem>
        )}
      </div>
    );
  }
}

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
import * as classes from "@app/uiTheme/radium/replacingRadiumPseudoClasses.module.less";

import SimpleButton from "components/Buttons/SimpleButton";

import config from "dyn-load/utils/config";

export default function (input) {
  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties
    getButtons() {
      const map = [
        config.supportEmailTo && [
          "email",
          <SimpleButton
            key="email-help"
            buttonStyle="secondary"
            className={clsx(classes["secondaryButtonPsuedoClasses"])}
            onClick={this.handleEmail}
          >
            {la("Email Help")}
          </SimpleButton>,
        ],
        [
          "download",
          <SimpleButton
            key="download-profile"
            buttonStyle="secondary"
            className={clsx(classes["secondaryButtonPsuedoClasses"])}
            submitting={this.props.downloadViewState.get("isInProgress")}
            onClick={this.handleDownload}
          >
            {la("Download Profile")}
          </SimpleButton>,
        ],
        this.props.isSupport &&
          this.props.clusterType === "YARN" && [
            "bundleDownload",
            <SimpleButton
              key="support-bundle"
              buttonStyle="secondary"
              className={clsx(classes["secondaryButtonPsuedoClasses"])}
              style={{ width: "220px" }}
              onClick={this.handleQueryDownload}
            >
              {la("Download Query Support Bundle")}
            </SimpleButton>,
          ],
      ];
      return new Immutable.OrderedMap(map);
    },
  });
}

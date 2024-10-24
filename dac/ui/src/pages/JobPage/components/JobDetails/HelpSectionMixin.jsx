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
import Immutable from "immutable";
import { Button, Tooltip } from "dremio-ui-lib/components";

import config from "dyn-load/utils/config";
import { shouldDisableDownload } from "@inject/pages/JobDetailsPageNew/utils";

export default function (input) {
  Object.assign(input.prototype, {
    getButtons() {
      const isDownloadDisabled = shouldDisableDownload(this.props.jobDetails);
      const DownloadButton = (
        <Button
          disabled={isDownloadDisabled}
          className="mx-05 mb-05"
          key="download-profile"
          variant="secondary"
          pending={this.props.downloadViewState.get("isInProgress")}
          onClick={this.handleDownload}
        >
          {laDeprecated("Download Profile")}
        </Button>
      );
      const map = [
        config.supportEmailTo && [
          "email",
          <Button
            className="mx-05 mb-05"
            key="email-help"
            variant="secondary"
            onClick={this.handleEmail}
          >
            {laDeprecated("Email Help")}
          </Button>,
        ],
        this.state.profileDownloadEnabled && [
          "download",
          isDownloadDisabled ? (
            <Tooltip
              shouldWrapChildren
              placement="top"
              content={this.props.intl.formatMessage({
                id: "Job.Summary.OutputIncomplete",
              })}
            >
              {DownloadButton}
            </Tooltip>
          ) : (
            DownloadButton
          ),
        ],
        this.props.isSupport &&
          this.props.clusterType === "YARN" && [
            "bundleDownload",
            <Button
              className="m-05"
              key="support-bundle"
              variant="secondary"
              onClick={this.handleQueryDownload}
            >
              {laDeprecated("Download Query Support Bundle")}
            </Button>,
          ],
      ];
      return new Immutable.OrderedMap(map);
    },
  });
}

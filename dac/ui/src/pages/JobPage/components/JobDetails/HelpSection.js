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
import Immutable from "immutable";
import { connect } from "react-redux";
import { injectIntl } from "react-intl";
import localStorageUtils from "utils/storageUtils/localStorageUtils";

import "./HelpSection.less";

import { askGnarly } from "actions/jobs/jobs";
import { callIfChatAllowedOrWarn } from "actions/account";
import { addNotification } from "actions/notification";

import { getViewState } from "selectors/resources";
import tokenUtils from "@inject/utils/tokenUtils";
import APICall from "@app/core/APICall";
import classNames from "clsx";
import jobsUtils from "utils/jobsUtils";
import config from "dyn-load/utils/config";

import HelpSectionMixin from "dyn-load/pages/JobPage/components/JobDetails/HelpSectionMixin";

// todo: loc
const MESSAGES = {
  chat: `Need help troubleshooting query performance or understanding the results?
     Click “Ask Dremio” to share the query profile with a Dremio engineer who will help you out.`,
  email: `Need help troubleshooting query performance or understanding the results?
Click “Email Help” to share job information with your system administrator.`,
  chatAndEmail: `Need help troubleshooting query performance or understanding the results?
Click “Ask Dremio” to share the query profile with Dremio Support or “Email Help” to share it with your system administrator.`,
  download: "Click “Download Profile” to download the query profile.",
  bundleDownload:
    "Click “Download Query Support Bundle” to download the query support.",
}; // download is never called out in the message unless it's the only thing

@injectIntl
@HelpSectionMixin
export class HelpSection extends PureComponent {
  static propTypes = {
    jobId: PropTypes.string.isRequired,
    downloadFile: PropTypes.func,
    intl: PropTypes.object.isRequired,

    // connected:
    callIfChatAllowedOrWarn: PropTypes.func.isRequired,
    askGnarly: PropTypes.func.isRequired,
    downloadViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    clusterType: PropTypes.string,
    addNotification: PropTypes.func,
    isSupport: PropTypes.bool,
    className: PropTypes.string,
  };

  static contextTypes = {
    loggedInUser: PropTypes.object,
  };

  handleDownload = () => {
    this.props.downloadFile(this.props.downloadViewState.get("viewId"));
  };

  showNotification() {
    const { intl } = this.props;
    const type = "Query Bundle";
    const notificationMessage = intl.formatMessage(
      { id: "Download.Notification" },
      { type }
    );
    const message = <span>{notificationMessage}</span>;
    this.props.addNotification(message, "success", 3);
  }

  handleQueryDownload = () => {
    this.showNotification();
    return tokenUtils
      .getTempToken({
        params: {
          durationSeconds: 30,
          request: "support-bundle/" + this.props.jobId + "/download/",
        },
        requestApiVersion: 3,
      })
      .then((data) => {
        const apiCall = new APICall()
          .path("support-bundle")
          .path(this.props.jobId)
          .path("download")
          .params({
            ".token": data.token ? data.token : "",
          });
        window.location.assign(apiCall.toString());
        return null;
      });
  };

  handleEmail = () => {
    const subject =
      config.supportEmailSubjectForJobs ||
      la("Can I get some help on a Dremio job?");
    const body =
      la("Hi,\n\nCan you help me with this Dremio job?\n\n") +
      window.location.origin +
      jobsUtils.navigationURLForJobId({
        id: this.props.jobId,
        windowLocationSearch: window.location.search,
      }) +
      "\n\n";
    const mailto =
      "mailto:" +
      encodeURIComponent(config.supportEmailTo) +
      "?subject=" +
      encodeURIComponent(subject) +
      "&body=" +
      encodeURIComponent(body);

    // We don't want the mailto to override the Dremio tab if the user uses webmail.
    // But we also don't want it to leave a blank tab if the user uses an app.
    // So we give the browser 5s to figure out how to handle it.
    // That's probably way too long most of the time, but it's gated on latency to your webmail. :(
    // Luckily, if you use webmail everything looks fine, and if you have native mail
    // you just switched to a completely different app.
    // Tested in Chrome/Mac, Firefox/Mac with Apple Mail and Gmail webmail.
    const newWindow = window.open(mailto, "_blank");
    setTimeout(() => {
      try {
        const href = newWindow.location.href; // will `throw` if webmail
        if (href === "about:blank") {
          newWindow.close();
        }
      } catch (e) {
        // loaded webmail and now security blocked
      }
    }, 5000);
  };

  render() {
    const buttons = this.getButtons();
    const { intl } = this.props;
    if (!buttons.size) return null;
    let message = MESSAGES.download;
    if (buttons.has("chat") && buttons.has("email")) {
      message = MESSAGES.chatAndEmail;
    } else if (buttons.has("chat")) {
      message = MESSAGES.chat;
    } else if (buttons.has("email")) {
      message = MESSAGES.email;
    }
    const helpSectionClassNames = classNames("helpSection", {
      "margin-left--none": buttons.size === 1,
    });
    return (
      <div className={helpSectionClassNames}>
        <h2>{intl.formatMessage({ id: "Help_Section" })}</h2>
        <div className="helpSection__quoteWrapper">
          <div className="helpSection__quoteBlock">
            <div className="helpSection__quoteMessage">{message}</div>
          </div>
          <div className="helpSection__buttons">
            {localStorageUtils.getIsQVJobs()
              ? buttons.toArray().reverse()
              : buttons.toArray()}
          </div>
        </div>
      </div>
    );
  }
}

function mapStateToProps(state, props) {
  return {
    downloadViewState: getViewState(
      state,
      `DOWNLOAD_JOB_PROFILE-${props.jobId}`
    ),
    clusterType: state.jobs.jobs.get("clusterType"),
    isSupport: state.jobs.jobs.get("isSupport"),
  };
}

export default connect(mapStateToProps, {
  askGnarly,
  callIfChatAllowedOrWarn,
  addNotification,
})(HelpSection);

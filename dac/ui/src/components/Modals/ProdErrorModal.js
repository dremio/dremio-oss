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
import clsx from "clsx";
import Modal from "components/Modals/Modal";
import { modalContent } from "uiTheme/radium/modal";
import FontIcon from "components/Icon/FontIcon";
import SimpleButton from "components/Buttons/SimpleButton";
import CopyButton from "components/Buttons/CopyButton";
import sentryUtil from "utils/sentryUtil";
import config from "dyn-load/utils/config";

import { modalFooter } from "uiTheme/radium/modal";
import { formDescription } from "uiTheme/radium/typography";
import * as classes from "@app/uiTheme/radium/replacingRadiumPseudoClasses.module.less";
import { FormattedMessage } from "react-intl";

export default class ProdErrorModal extends Component {
  static propTypes = {
    error: PropTypes.object.isRequired,
    eventId: PropTypes.string,
    onHide: PropTypes.func,
    showGoHome: PropTypes.bool,
  };

  renderCopyButton(valueToCopy) {
    return (
      !config.outsideCommunicationDisabled && (
        <CopyButton
          text={valueToCopy}
          title={<FormattedMessage id={"Common.Copy"} />}
          buttonStyle={styles.copyButton}
        />
      )
    );
  }

  render() {
    const { eventId, showGoHome } = this.props;

    const sessionUUID =
      laDeprecated("Session ID:") + " " + sentryUtil.sessionUUID;

    return (
      <Modal
        isOpen
        onClickCloseButton={
          this.props
            .onHide /* restrict closing to clicking close, instead of clicking off modal */
        }
        classQa="prod-error-modal"
        size="smallest"
        title={laDeprecated("An Unexpected Error Occurred")}
      >
        <div style={{ ...modalContent, ...styles.wrapper }}>
          <div style={styles.leftSide}>
            <FontIcon type="Error" iconStyle={{ width: 60, height: 60 }} />
          </div>
          <div style={styles.content}>
            <div>
              {laDeprecated("If the problem persists, please contact support.")}
            </div>
            <div style={{ ...formDescription, fontSize: 12, marginTop: "1em" }}>
              <div>
                {sessionUUID}
                {this.renderCopyButton(sentryUtil.sessionUUID)}
              </div>
              {eventId && (
                <div>
                  Event ID: {eventId}
                  {this.renderCopyButton(eventId)}
                </div>
              )}
            </div>
          </div>
        </div>

        <div
          style={{
            ...modalFooter,
            paddingTop: 12,
            display: "flex",
            justifyContent: "flex-end",
          }}
        >
          {showGoHome && (
            <SimpleButton
              data-qa="goHome"
              type="button"
              buttonStyle="secondary"
              className={clsx(classes["secondaryButtonPsuedoClasses"])}
              onClick={() => (window.location = "/")}
            >
              {laDeprecated("Go Home")}
            </SimpleButton>
          )}
          <SimpleButton
            data-qa="reload"
            type="button"
            buttonStyle="primary"
            className={clsx(classes["primaryButtonPsuedoClasses"])}
            onClick={() => window.location.reload()}
          >
            {laDeprecated("Reload")}
          </SimpleButton>
        </div>
      </Modal>
    );
  }
}

const styles = {
  wrapper: {
    flexDirection: "row",
    alignItems: "center",
  },
  leftSide: {
    padding: 10,
    width: 80,
  },
  copyButton: {
    height: "20px",
    width: "20px",
    padding: "2px",
    marginLeft: "var(--dremio--spacing--05)",
  },
};

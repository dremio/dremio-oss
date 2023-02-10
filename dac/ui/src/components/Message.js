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
import invariant from "invariant";
import Immutable from "immutable";
import Linkify from "linkifyjs/react";
import PropTypes from "prop-types";
import { Link } from "react-router";
import { FormattedMessage } from "react-intl";
import uuid from "uuid";

import { fixedWidthDefault } from "uiTheme/radium/typography";
import Modal from "components/Modals/Modal";
import ModalForm from "components/Forms/ModalForm";
import FormBody from "components/Forms/FormBody";

import jobsUtils from "utils/jobsUtils";
import { haveLocKey } from "utils/locale";

import "./Message.less";

export const RENDER_NO_DETAILS = Symbol("RENDER_NO_DETAILS");

class Message extends PureComponent {
  // must be a superset of the notification system `level` options
  static MESSAGE_TYPES = ["info", "success", "warning", "error"];

  static URLS_ALLOWED = {
    "https://docs.dremio.com/advanced-administration/log-files.html": true,
  };

  static defaultProps = {
    messageType: "info",
    isDismissable: true,
    message: Immutable.Map(),
    inFlow: true,
    useModalShowMore: false,
  };

  static propTypes = {
    className: PropTypes.any,
    message: PropTypes.oneOfType([
      PropTypes.node,
      PropTypes.instanceOf(Immutable.Map), // errors with extra details
    ]),
    messageType: PropTypes.string,
    dismissed: PropTypes.bool,
    onDismiss: PropTypes.func,
    style: PropTypes.object,
    messageTextStyle: PropTypes.object,
    messageId: PropTypes.string,
    detailsStyle: PropTypes.object,
    isDismissable: PropTypes.bool,
    inFlow: PropTypes.bool,
    useModalShowMore: PropTypes.bool,
    multilineMessage: PropTypes.bool,
    customRightButton: PropTypes.any,
  };

  constructor(props) {
    super(props);
    const messageText = `messageType must be one of ${JSON.stringify(
      Message.MESSAGE_TYPES
    )}`;
    invariant(
      Message.MESSAGE_TYPES.indexOf(this.props.messageType) !== -1,
      messageText
    );
  }

  state = {
    dismissed: false,
    showMore: false,
  };

  componentWillReceiveProps(nextProps) {
    if (this.props.messageId !== nextProps.messageId) {
      this.setState({ dismissed: false, showMore: false });
    }
  }

  onDismiss = () => {
    if (this.props.onDismiss) {
      if (this.props.onDismiss() === false) {
        return;
      }
    }
    this.setState({ dismissed: true });
  };

  prevent = (e) => {
    e.preventDefault();
    e.stopPropagation();
  };

  showMoreToggle = () => {
    this.setState((prevState) => {
      return { showMore: !prevState.showMore };
    });
  };

  renderErrorMessageText() {
    let messageText = this.props.message;
    if (messageText instanceof Immutable.Map) {
      // note: #errorMessage is legacy
      // fall back to #code (better than empty string)
      messageText =
        this.renderMessageForCode() ||
        messageText.get("message") ||
        messageText.get("errorMessage") ||
        messageText.get("code");
      if (typeof messageText === "string" && !messageText.endsWith(".")) {
        messageText += ".";
      }
    }

    return this.getMessage(messageText);
  }

  getMessage(messageText) {
    return this.props.multilineMessage
      ? this.injectNewLines(messageText)
      : messageText;
  }

  injectNewLines = (text) => {
    if (!text || typeof text !== "string") return text;

    return text.split("\n").reduce((arr, t, i) => {
      if (i > 0) {
        arr.push(<br />);
      }
      arr.push(t);
      return arr;
    }, []);
  };

  renderIcon(messageType) {
    const iconNameMap = {
      error: "interface/close-circle-error",
      warning: "interface/warning-circle-notf",
      info: "interface/circle-info",
      success: "interface/success-circle",
    };

    const altMap = {
      error: "Error",
      warning: "Warning",
      info: "Info",
      success: "Success",
    };
    return (
      <dremio-icon
        name={iconNameMap[messageType]}
        class="message-icon"
        alt={altMap[messageType]}
        data-qa={iconNameMap[messageType]}
      />
    );
  }

  renderDetails() {
    const message = this.props.message;
    if (!(message instanceof Immutable.Map)) {
      return;
    }

    let details = [];

    if (message.get("moreInfo") instanceof PropTypes.node) {
      return <>{message.get("moreInfo")}</>;
    }

    // If there's a messageForCode, show the #message in the details.
    if (this.renderMessageForCode()) {
      details.push(message.get("message"));
    }

    details.push(this.getMessage(message.get("moreInfo")));

    const codeDetails = this.renderDetailsForCode();
    if (codeDetails === RENDER_NO_DETAILS) {
      return;
    }
    details.push(codeDetails);

    let stackTrace = message.get("stackTrace");
    if (stackTrace) {
      if (stackTrace.join) {
        // handle arrays (todo: why not always use strings? does anything send arrays anymore?)
        stackTrace = stackTrace.join("\n");
      }
      details.push(
        <div style={{ ...styles.stackTrace, ...fixedWidthDefault }}>
          {stackTrace}
        </div>
      );
    }

    details = details.filter(Boolean);

    if (!details.length) return;

    const separatedStyle = {
      paddingTop: 10,
      marginTop: 10,
      borderTop: "1px solid hsla(0, 0%, 0%, 0.2)",
    };
    details = details.map((e, i) => (
      <div key={uuid()} style={i ? separatedStyle : {}}>
        {e}
      </div>
    ));

    return <div>{details}</div>;
  }

  renderMessageForCode() {
    // this fcn assumes we have already checked that we have an ImmutableMap
    const message = this.props.message;
    const code = message.get("code");
    if (!code) return;

    switch (code) {
      case "PIPELINE_FAILURE":
        return (
          <span>{la("There was an error in the Reflection pipeline.")}</span>
        );
      case "MATERIALIZATION_FAILURE": {
        const messageForCode = la("There was an error building a Reflection");
        // todo: #materializationFailure should become generic #details
        const url = jobsUtils.navigationURLForJobId({
          id: message.getIn(["materializationFailure", "jobId"]),
          windowLocationSearch: window.location.search,
        });
        return (
          <span>
            {messageForCode} (<Link to={url}>{la("show job")}</Link>).
          </span>
        ); // todo: better loc
      }
      case "DROP_FAILURE":
        return <span>{la("There was an error dropping a Reflection.")}</span>;
      case "COMBINED_REFLECTION_SAVE_ERROR": {
        const totalCount = message.getIn(["details", "totalCount"]);
        const countSuccess =
          totalCount - message.getIn(["details", "reflectionSaveErrors"]).size;
        const countFail = totalCount - countSuccess;
        return (
          <FormattedMessage
            id="Message.SomeReflectionsFailed"
            values={{ countSuccess, countFail }}
          />
        );
      }
      case "COMBINED_REFLECTION_CONFIG_INVALID":
        return (
          <span>
            {la(
              "Some Reflections had to be updated to work with the latest version of the dataset. Please review all Reflections before saving."
            )}
          </span>
        );
      case "REQUESTED_REFLECTION_MISSING":
        return <span>{la("The requested Reflection no longer exists.")}</span>;
      case "REFLECTION_LOST_FIELDS":
        return <span>{la("Review changes")}</span>;
      default: {
        const asLocKey = `Message.Code.${code}.Message`;
        if (haveLocKey(asLocKey)) return <FormattedMessage id={asLocKey} />;
      }
    }
  }

  renderDetailsForCode() {
    // this fcn assumes we have already checked that we have an ImmutableMap
    const message = this.props.message;
    const code = message.get("code");
    if (!code) return;

    switch (code) {
      case "MATERIALIZATION_FAILURE":
        return RENDER_NO_DETAILS; // job link has all the needed info
      default: {
        const asLocKey = `Message.Code.${code}.Details`;
        if (haveLocKey(asLocKey)) return <FormattedMessage id={asLocKey} />;
      }
    }
  }

  renderShowMoreToggle() {
    return (
      <span
        onClick={this.showMoreToggle}
        onMouseUp={this.prevent}
        style={{
          ...styles.showMoreLink,
          marginRight: this.props.isDismissable ? 30 : 5,
        }}
      >
        <FormattedMessage
          id={this.state.showMore ? "Message.Show.Less" : "Message.Show.More"}
        />
      </span>
    );
  }

  renderShowMore(details, linkOptions) {
    const { messageType } = this.props;
    if (!details) return null;
    if (!this.props.useModalShowMore)
      return (
        this.state.showMore && (
          <div
            className="message-content"
            style={{
              ...styles.details,
              ...(this.props.detailsStyle || {}),
              ...styles[messageType],
            }}
          >
            <Linkify options={linkOptions}>{details}</Linkify>
          </div>
        )
      );

    const hide = () => this.setState({ showMore: false });

    return (
      <Modal
        size="small"
        title={this.renderErrorMessageText()}
        isOpen={this.state.showMore}
        hide={hide}
      >
        <ModalForm onSubmit={hide} confirmText={la("Close")} isNestedForm>
          {" "}
          {/* best to assume isNestedForm */}
          <FormBody className="message-content">
            <Linkify options={linkOptions}>{details}</Linkify>
          </FormBody>
        </ModalForm>
      </Modal>
    );
  }

  render() {
    const {
      className,
      messageType,
      style,
      messageTextStyle,
      inFlow,
      isDismissable,
      customRightButton,
    } = this.props;

    if (
      this.props.dismissed ||
      (this.props.dismissed === undefined && this.state.dismissed)
    ) {
      return null;
    }

    const details = this.renderDetails();
    const linkOptions = {
      validate: {
        url: (url) => {
          return Message.URLS_ALLOWED[url];
        },
        email: () => false,
      },
    };

    return (
      <div
        className={`message ${messageType} ${
          className || ""
        } margin-bottom--double`}
        style={{
          ...styles.wrap,
          ...(!inFlow && styles.notInFlow),
          ...(style || {}),
        }}
      >
        <div style={{ ...styles.base, ...styles[messageType] }}>
          {this.renderIcon(messageType)}
          <span
            className="message-content"
            style={{ ...styles.messageText, ...messageTextStyle }}
            onMouseUp={this.prevent}
          >
            <Linkify options={linkOptions}>
              {this.renderErrorMessageText()}
            </Linkify>
            {details && this.renderShowMoreToggle()}
          </span>
          {isDismissable && (
            <div style={styles.rightButton}>
              <dremio-icon
                name="interface/close-small"
                alt="Dismiss"
                onClick={this.onDismiss}
                class="dismiss-btn"
              />
            </div>
          )}
          {!isDismissable && customRightButton ? (
            <div style={styles.rightButton}>{customRightButton}</div>
          ) : null}
        </div>
        {this.renderShowMore(details, linkOptions)}
      </div>
    );
  }
}

const styles = {
  wrap: {
    width: "100%",
  },
  notInFlow: {
    position: "absolute",
    top: 0,
    width: "100%",
  },
  base: {
    display: "flex",
    flexWrap: "nowrap",
    width: "100%",
    borderRadius: 1,
    MozUserSelect: "text",
    WebkitUserSelect: "text",
    UserSelect: "text",
    position: "relative",
    minHeight: 32,
    zIndex: 100, // needs to be above disabled table overlay
  },
  messageText: {
    flexGrow: 1,
    maxHeight: 100,
    overflowY: "auto",
    minHeight: 32,
    lineHeight: "18px",
    fontSize: "12px",
    color: "#202124",
    padding: "8px 0", // add our own padding for scroll reasons
  },
  stackTrace: {
    whiteSpace: "pre",
    maxWidth: 400,
  },
  showMoreLink: {
    cursor: "pointer",
    marginLeft: 10,
    textDecoration: "underline",
    flexShrink: 0,
  },
  details: {
    padding: "10px 38px",
    maxHeight: 200,
    width: "100%",
    overflowX: "auto",
    backgroundColor: "#FDEDED",
  },
  rightButton: {
    justifyContent: "center",
    display: "flex",
    alignItems: "center",
    height: 24,
    width: 24,
    padding: 5,
    marginRight: 12,
    marginLeft: 12,
    marginTop: 6,
  },
  icon: {
    marginRight: 8,
    marginLeft: 12,
    height: 20,
    marginTop: 6,
  },
  msgWrap: {
    lineHeight: "24px",
    wordWrap: "break-word",
    display: "inline-block",
    width: "100%",
  },
  info: {
    backgroundColor: "#E9F5F9",
  },

  success: {
    backgroundColor: "#EDF7ED",
  },

  warning: {
    backgroundColor: "#FFF4E5",
  },

  error: {
    backgroundColor: "#FDEDED",
  },
};
export default Message;

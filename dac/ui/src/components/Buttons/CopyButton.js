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
import { connect } from "react-redux";
import CopyToClipboard from "react-copy-to-clipboard";
import { addNotification } from "actions/notification";
import CopyButtonIcon from "@app/components/Buttons/CopyButtonIcon";
import { MSG_CLEAR_DELAY_SEC } from "@app/constants/Constants";

export class CopyButton extends Component {
  static propTypes = {
    text: PropTypes.string.isRequired,
    title: PropTypes.string,
    style: PropTypes.object,
    addNotification: PropTypes.func.isRequired,
    iconVersion: PropTypes.number,
    showCopiedContent: PropTypes.bool,
    iconStyle: PropTypes.object,
  };

  static defaultProps = {
    title: "Copy Path", // TODO: loc
    showCopiedContent: true,
  };

  handleCopy = () => {
    const { showCopiedContent } = this.props;
    const message = (
      <span>Copied {showCopiedContent && <i>{this.props.text}</i>}.</span>
    ); // TODO: loc
    this.props.addNotification(message, "success", MSG_CLEAR_DELAY_SEC);
  };

  render() {
    const { text, title, style, iconVersion, iconStyle } = this.props;
    return (
      <CopyToClipboard text={text} onCopy={this.handleCopy}>
        <CopyButtonIcon
          title={title}
          style={style}
          version={iconVersion}
          iconStyle={iconStyle}
        />
      </CopyToClipboard>
    );
  }
}

export default connect(null, {
  addNotification,
})(CopyButton);

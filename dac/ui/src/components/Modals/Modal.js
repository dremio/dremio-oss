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
import ReactModal from "react-modal";
import ReactDOMServer from "react-dom/server";

import {
  smallModal,
  mediumModal,
  largeModal,
  tallModal,
  smallestModal,
  modalBody,
} from "uiTheme/radium/modal";

import ModalHeader from "./ModalHeader";
import "./Modals.less";

// react-modal recommends to set app element. It put warnings in console without this.
ReactModal.setAppElement(document.body);

export const ModalSize = {
  small: "small",
  large: "large",
  tall: "tall",
  medium: "medium",
  smallest: "smallest",
};

export const MODAL_CLOSE_ANIMATION_DURATION = 150;
export default class Modal extends Component {
  constructor(props) {
    super(props);
    this.state = {
      addHeaderShadow: false,
    };
  }
  static propTypes = {
    size: PropTypes.oneOf(Object.values(ModalSize)).isRequired,
    isOpen: PropTypes.bool,
    hideCloseButton: PropTypes.bool,
    onClickCloseButton: PropTypes.func, // optional. defaults to props.hide
    hide: PropTypes.func,
    children: PropTypes.node,
    title: PropTypes.node.isRequired,
    modalHeight: PropTypes.string,
    className: PropTypes.string,
    classQa: PropTypes.string,
    style: PropTypes.object,
    dataQa: PropTypes.string,
    headerClassName: PropTypes.string,
    headerEndChildren: PropTypes.node,
    closeButtonType: PropTypes.string,
    headerIcon: PropTypes.node,
  };

  static defaultProps = {
    title: "",
    classQa: "",
    hideCloseButton: false,
  };

  render() {
    const {
      size,
      isOpen,
      title,
      hide,
      hideCloseButton,
      onClickCloseButton,
      className,
      children,
      style,
      classQa,
      dataQa,
      headerClassName,
      headerEndChildren,
      closeButtonType,
      headerIcon,
    } = this.props;
    const content = {
      ...smallModal.content,
      height: this.props.modalHeight || smallModal.content.height,
      ...style,
    };

    const smallModalUpdated = { ...smallModal, content };
    const smallestModalUpdated = {
      ...smallestModal,
      content: { ...smallestModal.content, ...style },
    };
    const mediumModalUpdated = {
      ...mediumModal,
      content: { ...mediumModal.content, ...style },
    };
    const largeModalUpdated = {
      ...largeModal,
      content: { ...largeModal.content, ...style },
    };
    const tallModalUpdated = {
      ...tallModal,
      content: { ...tallModal.content, ...style },
    };
    const styles = {
      [ModalSize.small]: smallModalUpdated,
      [ModalSize.medium]: mediumModalUpdated,
      [ModalSize.large]: largeModalUpdated,
      [ModalSize.tall]: tallModalUpdated,
      [ModalSize.smallest]: smallestModalUpdated,
    };

    let stringTitle = title;
    if (typeof stringTitle === "object") {
      const html = ReactDOMServer.renderToStaticMarkup(stringTitle);
      const tmp = document.createElement("div");
      tmp.innerHTML = html;
      stringTitle = tmp.textContent;
    }

    const onScroll = (e) => {
      const scrollTop = e.currentTarget.scrollTop;
      scrollTop > 0
        ? this.setState({ addHeaderShadow: true })
        : this.setState({ addHeaderShadow: false });
    };

    return (
      <ReactModal
        contentLabel={stringTitle}
        overlayClassName={`dremio-modal ${size}-modal qa-${classQa}`}
        isOpen={isOpen}
        onRequestClose={hide}
        style={styles[size]}
        closeTimeoutMS={MODAL_CLOSE_ANIMATION_DURATION}
        className={className}
      >
        {stringTitle && (
          <ModalHeader
            title={stringTitle}
            hide={onClickCloseButton || hide}
            hideCloseButton={hideCloseButton}
            className={headerClassName}
            endChildren={headerEndChildren}
            type={closeButtonType}
            headerIcon={headerIcon}
            addShadow={this.state.addHeaderShadow}
          ></ModalHeader>
        )}
        <div style={modalBody} data-qa={dataQa} onScroll={(e) => onScroll(e)}>
          {children}
        </div>
      </ReactModal>
    );
  }
}

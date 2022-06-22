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
import React, { Fragment } from "react";
import PropTypes from "prop-types";
import classNames from "classnames";

import { SelectView } from "@app/components/Fields/SelectView";

import { triangleTop } from "uiTheme/radium/overlay";
import Spinner from "@app/components/Spinner";
import { Divider } from "@material-ui/core";
import { Button } from "dremio-ui-lib";

import "./DropdownMenu.less";
import Art from "../Art";

const DropdownMenu = (props) => {
  const {
    dataQa,
    className,
    text,
    textTooltip,
    iconType,
    menu,
    style,
    textStyle,
    fontIcon,
    hideArrow,
    hideTopArrow,
    disabled,
    isButton,
    isSolidButton,
    iconTooltip,
    arrowStyle,
    customItemRenderer,
    listStyle,
    customImage,
    isDownloading,
    listClass,
    groupDropdownProps,
    selectClass,
    closeOnSelect,
  } = props;

  const togglerStyle = isButton
    ? "dropdownMenu__togglerButton"
    : "dropdownMenu__toggler";
  const stdArrowStyle = isButton ? styles.downButtonArrow : styles.downArrow;
  const menuListStyle = listStyle ? listStyle : styles.popover;

  const selectedItemRenderer = () => (
    <>
      {text && (
        <span className="dropdownMenu__text" style={{ ...textStyle }}>
          {text}
        </span>
      )}

      {iconType && (
        <div className="dropdownMenu__iconWrap">
          <Art
            src="Download.svg"
            alt=""
            title="Download"
            className="dropdownMenu__icon"
          />
        </div>
      )}

      {fontIcon && (
        <div>
          <div className={fontIcon} title={iconTooltip} />
        </div>
      )}

      {customImage && <div>{customImage}</div>}
    </>
  );

  return (
    <div
      className={classNames(
        "dropdownMenu",
        isButton ? "--button" : "",
        isSolidButton ? "--solidButton" : "",
        disabled ? "--disabled" : ""
      )}
      style={{ ...style }}
    >
      {groupDropdownProps && (
        <>
          <Button
            disableMargin
            onClick={groupDropdownProps.onClick}
            className={classNames(
              "dropdownMenu__groupButton",
              disabled ? "--disabled" : ""
            )}
          >
            {groupDropdownProps.text}
          </Button>
          <Divider
            orientation="vertical"
            className="dropdownMenu__groupDivider"
          />
        </>
      )}
      {isDownloading ? (
        <Spinner />
      ) : (
        <SelectView
          className={selectClass}
          content={
            <div
              className={classNames(
                "dropdownMenu__content",
                className,
                togglerStyle
              )}
              key="toggler"
              title={textTooltip}
            >
              {!groupDropdownProps &&
                (customItemRenderer || selectedItemRenderer())}
              {!hideArrow && (
                <i
                  className="fa fa-angle-down"
                  style={{ ...stdArrowStyle, ...arrowStyle }}
                />
              )}
            </div>
          }
          hideExpandIcon
          listStyle={menuListStyle}
          listRightAligned
          dataQa={dataQa}
          disabled={disabled}
          listClass={listClass}
          closeOnSelect={closeOnSelect}
        >
          {({ closeDD }) => {
            return (
              <Fragment>
                {!hideTopArrow && <div style={styles.triangle} />}
                {React.cloneElement(menu, { closeMenu: closeDD })}
              </Fragment>
            );
          }}
        </SelectView>
      )}
    </div>
  );
};

const styles = {
  icon: {
    Icon: {
      width: 17,
      height: 17,
    },
    Container: {
      width: 17,
      height: 17,
      display: "block",
    },
  },
  downArrow: {
    fontSize: "18px",
    position: "relative",
    top: "1px",
    marginLeft: "5px",
  },
  downButtonArrow: {
    fontSize: 17,
  },
  popover: {
    marginTop: 7,
    overflow: "hidden auto",
  },
  triangle: {
    ...triangleTop,
    right: 11,
  },
  divider: {
    height: 20,
    margin: "0 4px",
    borderLeft: "1px solid #E5E5E5",
    display: "block",
  },
};

DropdownMenu.propTypes = {
  dataQa: PropTypes.string,
  className: PropTypes.string,
  text: PropTypes.string,
  iconType: PropTypes.string,
  menu: PropTypes.node.isRequired,
  style: PropTypes.object,
  iconStyle: PropTypes.object,
  textStyle: PropTypes.object,
  textTooltip: PropTypes.string,
  hideArrow: PropTypes.bool,
  hideTopArrow: PropTypes.bool,
  arrowStyle: PropTypes.object,
  disabled: PropTypes.bool,
  isButton: PropTypes.bool,
  isSolidButton: PropTypes.bool,
  iconTooltip: PropTypes.string,
  fontIcon: PropTypes.string,
  customItemRenderer: PropTypes.element,
  customImage: PropTypes.object,
  listStyle: PropTypes.object,
  isDownloading: PropTypes.bool,
  listClass: PropTypes.string,
  groupDropdownProps: PropTypes.object,
  closeOnSelect: PropTypes.bool,
  selectClass: PropTypes.string,
};

export default DropdownMenu;

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
import classNames from "clsx";
import { get } from "lodash/object";
import Popover from "@mui/material/Popover";
import Popper from "@mui/material/Popper";
import Paper from "@mui/material/Paper";
import ClickAwayListener from "@mui/material/ClickAwayListener";
import { generateEnumFromList } from "@app/utils/enumUtils";
import {
  popper as popperCls,
  popperPaper as popperPaperCls,
} from "./Popover.less";

const supportedMouseEvents = ["onClick", "onMouseDown", "onMouseUp"];
/**
 * Click away mouse event type, which is applied to {@see}
 */
export const MouseEvents = generateEnumFromList(supportedMouseEvents);

/**
 * It is an adapter to material-ui/core Popover and Popper.
 * In older version a popover had useLayerForClickAway property. In new version we should switch
 * usage from Popover to Popper in case useLayerForClickAway=true
 */
class DremioPopoverAdapter extends PureComponent {
  static propTypes = {
    children: PropTypes.node,
    useLayerForClickAway: PropTypes.bool,
    anchorEl: PropTypes.object, // todo reuse from Popover?
    style: PropTypes.object,
    listClass: PropTypes.string,
    listStyle: PropTypes.object,
    /** set to true if you want to align a popover to the right side of the content */
    listRightAligned: PropTypes.bool,
    onClose: PropTypes.func,
    listWidthSameAsAnchorEl: PropTypes.bool,
    dataQa: PropTypes.string,
    /** Applies to {@see Popper} instance if {@see useLayerForClickAway} = {@see true}*/
    clickAwayMouseEvent: PropTypes.oneOf(supportedMouseEvents),
    popoverFilters: PropTypes.string,
    classes: PropTypes.object,
    container: PropTypes.any,
  };

  static defaultProps = {
    mouseEvent: MouseEvents.onClick,
  };

  getListStyle = () => {
    const { listStyle, listWidthSameAsAnchorEl, anchorEl } = this.props;

    if (listWidthSameAsAnchorEl) {
      return { ...listStyle, width: get(anchorEl, "offsetWidth") };
    }
    return listStyle;
  };

  render() {
    const {
      useLayerForClickAway,
      anchorEl,
      children,
      dataQa,
      listClass,
      listRightAligned,
      onClose,
      style,
      classes,
      // popper specific props
      clickAwayMouseEvent,
      popoverFilters,
      container,
    } = this.props;

    const commonProps = {
      open: Boolean(anchorEl),
      anchorEl,
      "data-qa": dataQa,
    };

    // style = undefined in the commonProps breaks a layout. So we need to set style property only
    // if style is not empty
    if (style) {
      commonProps.style = style;
    }

    return useLayerForClickAway ? (
      <Popover
        className={popoverFilters}
        anchorOrigin={{
          horizontal: listRightAligned ? "right" : "left",
          vertical: "bottom",
        }}
        transformOrigin={{
          horizontal: listRightAligned ? "right" : "left",
          vertical: "top",
        }}
        onClose={onClose}
        transitionDuration={0}
        PaperProps={{
          style: this.getListStyle(),
          classes: { root: listClass },
        }}
        classes={{
          root: classes && classes.root,
        }}
        {...commonProps}
      >
        {children}
      </Popover>
    ) : (
      <Popper
        className={popperCls}
        placement={listRightAligned ? "bottom-end" : "bottom-start"}
        classes={{
          root: classes && classes.root,
        }}
        {...commonProps}
        container={container}
      >
        <ClickAwayListener
          mouseEvent={clickAwayMouseEvent}
          onClickAway={onClose}
        >
          <Paper
            className={classNames(listClass, popperPaperCls)}
            style={{ position: "relative", ...this.getListStyle() }}
          >
            {/* TODO <EventListener target='window' onResize={this.handleResize} /> */}
            {children}
          </Paper>
        </ClickAwayListener>
      </Popper>
    );
  }
}

export { DremioPopoverAdapter as Popover };

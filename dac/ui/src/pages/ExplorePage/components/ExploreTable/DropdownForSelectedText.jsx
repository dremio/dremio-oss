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
import { createRef, PureComponent } from "react";
import ReactDOM from "react-dom";
import Mousetrap from "mousetrap";
import Immutable from "immutable";
import PropTypes from "prop-types";
import { TEXT, LIST } from "#oss/constants/DataTypes";
import exploreUtils from "utils/explore/exploreUtils";
import { withLocation } from "containers/dremioLocation";

import { showCuration } from "@inject/pages/ExplorePage/utils";

import SelectedTextPopover from "./SelectedTextPopover";

const PADDING_TOP_FOR_TEXT = -2;
const PADDING_TOP_FOR_NUMB = 4;
const PADDING_SELECTED_NUMB = 5;
const PADDING_SELECTED_TEXT = 5;

export class DropdownForSelectedTextView extends PureComponent {
  static propTypes = {
    hideDrop: PropTypes.func.isRequired,
    dropPositions: PropTypes.instanceOf(Immutable.Map),
    openPopover: PropTypes.bool,
    location: PropTypes.object.isRequired,
  };

  constructor(props) {
    super(props);
    this.selectedTextRef = createRef();
    this.selectedContentRef = createRef();
    this.state = {
      anchor: null,
    };
  }

  componentDidMount() {
    this.updateAnchor();
    Mousetrap.bind(["command+c", "ctrl+c"], this.copyText);
  }

  componentDidUpdate() {
    this.updateAnchor();
  }

  componentWillUnmount() {
    Mousetrap.unbind(["command+c", "ctrl+c"]);
  }

  copyText = () => {
    const { dropPositions } = this.props;
    const text = dropPositions.getIn(["textWrap", "text"]);
    exploreUtils.copySelection(
      text ?? ReactDOM.findDOMNode(this.selectedContentRef.current),
    );
    this.props.hideDrop();
  };

  updateAnchor() {
    this.setState({ anchor: this.selectedTextRef.current });
  }

  stopPropagation(e) {
    e.stopPropagation();
  }

  render() {
    const { dropPositions, location } = this.props;
    const columnType = location.state.columnType;
    const padding =
      columnType !== TEXT && columnType !== LIST
        ? PADDING_TOP_FOR_NUMB
        : PADDING_TOP_FOR_TEXT;
    const paddingSelected =
      columnType !== TEXT && columnType !== LIST
        ? PADDING_SELECTED_NUMB
        : PADDING_SELECTED_TEXT;
    const textStyle = {
      display: dropPositions.get("display") ? "block" : "none",
      left: dropPositions.get("textWrap").get("left"),
      top: dropPositions.get("textWrap").get("top") + padding,
      width: dropPositions.get("textWrap").get("width"),
      maxHeight: 200,
      textAlign: "left",
      lineHeight: "15px",
      overflowX: "auto",
      position: "fixed",
      zIndex: 2001,
    };
    const selectedText = this.props.openPopover ? (
      <span
        className="selected-text"
        ref={this.selectedTextRef}
        style={{
          ...textStyle,
          backgroundColor: "var(--fill--tertiary)",
        }}
      >
        <div
          ref={this.selectedContentRef}
          style={{ paddingTop: paddingSelected }}
        >
          {dropPositions.get("textWrap").get("text")}
        </div>
        <dremio-icon
          name="interface/down-chevron"
          style={{ height: 14, width: 14 }}
        />
      </span>
    ) : null;
    return (
      <div onMouseUp={this.stopPropagation} onMouseDown={this.stopPropagation}>
        {selectedText}
        <SelectedTextPopover
          /* SelectedTextPopover ignores visibleItems if its null, undefined or empty array.
          So null will actually show all the action items and
          array with null as element will not show any items
          */
          visibleItems={showCuration() ? null : [null]}
          anchor={this.state.anchor}
          copySelection={this.copyText}
          hideDrop={this.props.hideDrop}
          columnName={location.query.column}
        />
      </div>
    );
  }
}

export default withLocation(DropdownForSelectedTextView);

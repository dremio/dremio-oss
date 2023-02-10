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
import Immutable from "immutable";

import FontIcon from "components/Icon/FontIcon";
import PropTypes from "prop-types";
import Meter from "components/Meter";
import { TEAL, BLACK } from "uiTheme/radium/colors";

const PROGRESS_WIDTH = 455;

class CardFooter extends PureComponent {
  static propTypes = {
    card: PropTypes.instanceOf(Immutable.Map),
    style: PropTypes.object,
  };

  static defaultProps = {
    card: Immutable.Map(),
  };

  constructor(props) {
    super(props);
  }

  renderFooter() {
    const { card, style } = this.props;
    const themeMathed = {
      Icon: { backgroundColor: "#2A394A", ...styles.iconTheme },
      Container: {
        ...styles.mathed,
        height: 8,
        width: 8,
        backgroundColor: "none",
      },
    };
    const max = card.get
      ? card.get("unmatchedCount") + card.get("matchedCount")
      : 0;
    const value = card.get ? card.get("matchedCount") : 0;
    const themeUnmathed = {
      Icon: styles.iconTheme,
      Container: { ...styles.mathed, height: 8, width: 8 },
    };
    const progressWidth = { width: style ? style.width : PROGRESS_WIDTH };

    return (
      <div
        className="match-panel"
        style={{ ...progressWidth, ...styles.labels, ...style }}
      >
        <FontIcon theme={themeUnmathed} />
        <span style={{ ...styles.text, marginLeft: 5 }}>
          {card.get && card.get("matchedCount")}
        </span>
        <span style={styles.text}>matched values</span>
        <FontIcon theme={themeMathed} />
        <span style={{ ...styles.text, marginLeft: 5 }}>
          {card.get && card.get("unmatchedCount")}
        </span>
        <span style={styles.text}>unmatched values</span>
        <Meter
          value={value}
          max={max}
          background={BLACK}
          style={{ ...styles.progress, ...progressWidth }}
        />
      </div>
    );
  }

  render() {
    const { style = {} } = this.props;
    return (
      <div style={{ ...styles.base, ...style }}>{this.renderFooter()}</div>
    );
  }
}

const styles = {
  base: {
    bottom: 0,
  },
  mathed: {
    margin: "-2px 0 1px 10px",
    backgroundColor: TEAL,
  },
  iconTheme: {
    fontSize: 8,
    width: 8,
    height: 8,
  },
  labels: {
    display: "flex",
    flexWrap: "wrap",
    alignItems: "center",
  },
  progress: {
    height: 7,
  },
  text: {
    marginLeft: 5,
    fontSize: 11,
    color: "#999999",
    display: "inline-block",
    height: 16,
  },
};
export default CardFooter;

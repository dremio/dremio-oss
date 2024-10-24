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

import PropTypes from "prop-types";
import Meter from "components/Meter";

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
    const max = card.get
      ? card.get("unmatchedCount") + card.get("matchedCount")
      : 0;
    const value = card.get ? card.get("matchedCount") : 0;
    const progressWidth = { width: style ? style.width : PROGRESS_WIDTH };

    return (
      <div
        className="match-panel"
        style={{ ...progressWidth, ...styles.labels, ...style }}
      >
        <div
          className="ml-1"
          style={{ ...styles.matched, height: 8, width: 8 }}
        />
        <span className="ml-05" style={{ ...styles.text }}>
          {card.get && card.get("matchedCount")}
        </span>
        <span style={styles.text}>matched values</span>
        <div
          className="ml-1"
          style={{
            ...styles.matched,
            height: 8,
            width: 8,
            backgroundColor: "black",
          }}
        />
        <span className="ml-05" style={{ ...styles.text }}>
          {card.get && card.get("unmatchedCount")}
        </span>
        <span style={styles.text}>unmatched values</span>
        <Meter
          value={value}
          max={max}
          background="black"
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
  matched: {
    backgroundColor: "#5ED7B9",
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
    marginLeft: "var(--dremio--spacing--05)",
    fontSize: 11,
    color: "var(--text--primary)",
    display: "inline-block",
    height: 16,
  },
};
export default CardFooter;

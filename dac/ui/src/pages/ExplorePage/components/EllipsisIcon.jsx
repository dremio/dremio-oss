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
import { IconButton } from "dremio-ui-lib/components";

export default class EllipsisIcon extends PureComponent {
  static propTypes = {
    onClick: PropTypes.func,
  };

  constructor(props) {
    super(props);
    this.onClick = this.onClick.bind(this);
  }
  onClick(e) {
    e.preventDefault();
    e.stopPropagation();
    this.props.onClick(e.currentTarget);
  }
  render() {
    return (
      <div className="ellipsis-icon">
        <IconButton aria-label="More..." onClick={this.onClick}>
          <div style={styles.ellipsis.Container}>
            <dremio-icon
              name="interface/more"
              style={styles.ellipsis.Icon}
            ></dremio-icon>
          </div>
        </IconButton>
      </div>
    );
  }
}

const styles = {
  ellipsis: {
    Container: {
      cursor: "pointer",
      borderRadius: 100,
      backgroundColor: "var(--fill--tertiary)",
      zIndex: 9000,
    },
    Icon: {
      blockSize: 13,
      inlineSize: 15,
    },
  },
};

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

import Select from "components/Fields/Select";
import { FieldWithError, TextField, Checkbox } from "components/Fields";

import {
  LINE_START_START,
  FLEX_COL_START_START,
} from "uiTheme/radium/flexStyle";

class SplitContent extends PureComponent {
  static propTypes = {
    matchType: PropTypes.object,
    ignoreCase: PropTypes.object,
    pattern: PropTypes.object,
  };

  constructor(props) {
    super(props);

    this.options = [
      {
        label: "Fixed String",
        option: "exact",
      },
      {
        label: "Regular Expression",
        option: "regex",
      },
    ];
  }

  renderContent() {
    const { pattern, matchType, ignoreCase } = this.props;
    const foundItem = this.options.find(
      (item) => item.option === matchType.value
    );
    const defaultSelect = foundItem && foundItem.label;
    return (
      <div style={{ ...LINE_START_START, ...styles.base }}>
        <Select
          dataQa="SplitSelect"
          items={this.options}
          style={styles.select}
          {...matchType}
          defaultValue={defaultSelect}
        />
        <div style={FLEX_COL_START_START}>
          <FieldWithError {...pattern} errorPlacement="bottom">
            <TextField
              data-qa="SplitText"
              style={styles.textField}
              {...pattern}
            />
          </FieldWithError>
          <Checkbox
            data-qa="SplitIgnoreCase"
            style={styles.check}
            label={laDeprecated("Ignore Case")}
            {...ignoreCase}
          />
        </div>
      </div>
    );
  }

  render() {
    return this.renderContent();
  }
}

const styles = {
  base: {
    marginLeft: 10,
  },
  select: {
    width: 140,
    background: "white",
  },
  textField: {
    width: 230,
    marginLeft: 10,
  },
  check: {
    marginLeft: 10,
  },
};
export default SplitContent;

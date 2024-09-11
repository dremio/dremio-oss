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
import { injectIntl } from "react-intl";

import { Checkbox } from "components/Fields";

@injectIntl
export default class ParquetFormatForm extends Component {
  static propTypes = {
    fields: PropTypes.object,
    disabled: PropTypes.bool,
    intl: PropTypes.object.isRequired,
  };

  static getFields() {
    return ["ignoreOtherFileFormats"];
  }

  render() {
    const {
      intl,
      disabled,
      fields: {
        Parquet: { ignoreOtherFileFormats },
      },
    } = this.props;

    return (
      <div>
        <div style={styles.row}>
          <div style={styles.options}>
            <Checkbox
              disabled={disabled}
              style={styles.checkbox}
              label={intl.formatMessage({ id: "File.IgnoreOtherFileFormats" })}
              {...ignoreOtherFileFormats}
            />
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  row: {
    display: "flex",
    flexWrap: "wrap",
    marginBottom: 10,
  },
  options: {
    display: "flex",
    alignItems: "center",
    height: 28,
  },
  checkbox: {
    marginRight: 10,
  },
};

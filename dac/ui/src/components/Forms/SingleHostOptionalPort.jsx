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

import { applyValidators, isRequired, isWholeNumber } from "utils/validation";
import FormUtils from "#oss/utils/FormUtils/FormUtils";

import Host from "./Host";

const { CONFIG_PROP_NAME } = FormUtils;

export default class SingleHostOptionalPort extends Component {
  static getFields() {
    return Host.getFields().map(FormUtils.addFormPrefixToPropName);
  }

  static propTypes = Host.propTypes;

  static validate(values) {
    return {
      [CONFIG_PROP_NAME]: applyValidators(values[CONFIG_PROP_NAME], [
        isRequired("hostname"),
        isWholeNumber("port"),
      ]),
    };
  }

  render() {
    const {
      fields: { [CONFIG_PROP_NAME]: configFields },
      ...otherProps
    } = this.props;
    return <Host fields={configFields} {...otherProps} />;
  }
}

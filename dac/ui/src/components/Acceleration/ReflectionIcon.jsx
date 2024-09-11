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
import Immutable from "immutable";
import { Tooltip } from "dremio-ui-lib";
import { formatMessage } from "utils/locale";
import { getIconPath } from "@app/utils/getIconPath";

export default class ReflectionIcon extends Component {
  static propTypes = {
    reflection: PropTypes.object.isRequired,
    style: PropTypes.object,
  };

  render() {
    const { reflection, style } = this.props;

    let icon = "";
    let text = "";

    const type = Immutable.Map.isMap(reflection)
      ? reflection.get("type")
      : reflection.type;

    if (type === "RAW") {
      text = "Reflection.Raw";
      icon = "interface/reflection-raw-mode";
    } else if (type === "AGGREGATION") {
      text = "Reflection.Aggregation";
      icon = "interface/reflection-aggregate";
    } else if (type === "EXTERNAL") {
      text = "Reflection.External";
      icon = "interface/physical-dataset-gray";
    }

    return (
      <Tooltip title={formatMessage(text)}>
        <img
          src={getIconPath(icon)}
          data-qa={icon}
          alt={formatMessage(text)}
          style={{ height: 24, width: 24, ...style }}
        />
      </Tooltip>
    );
  }
}

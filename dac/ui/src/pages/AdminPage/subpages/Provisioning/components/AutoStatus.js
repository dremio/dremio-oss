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
import PropTypes from "prop-types";

import Art from "@app/components/Art";

export default function AutoStatus(props) {
  const { value, style, offIcon } = props;
  // off icon is shown as '-' in the list and as "x" in the engine status summary bar
  // the x icon is passed in as an optional param
  const iconSrc = value ? "SimpleCheckMark.svg" : offIcon || "SimpleMinus.svg";
  const size = offIcon || value ? 24 : 9; //SimpleMinus.svg has no padding
  const pad = offIcon || value ? 0 : 7;
  const altText = value ? la("On") : la("Off");
  return (
    <Art
      src={iconSrc}
      style={{ height: size, width: size, marginLeft: pad, ...style }}
      alt={altText}
      title
    />
  );
}

AutoStatus.propTypes = {
  value: PropTypes.bool,
  style: PropTypes.object,
  offIcon: PropTypes.string,
};

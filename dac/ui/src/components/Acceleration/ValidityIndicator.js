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
import "@app/uiTheme/less/Acceleration/Acceleration.less";
import FontIcon from "components/Icon/FontIcon";

// todo: ax
export default function ValidityIndicator({ isValid }) {
  return (
    <span
      className={"ValidityIndicator"}
      title={
        isValid
          ? la("This Reflection is ready to accelerate queries.")
          : la("This Reflection is not ready to accelerate queries.")
      }
    >
      <FontIcon
        theme={themes.flameIcon}
        type={isValid ? "Flame" : "Flame-Disabled"}
      />
    </span>
  );
}

ValidityIndicator.propTypes = {
  isValid: PropTypes.bool,
};

const themes = {
  flameIcon: {
    Icon: {
      width: 13,
      height: 20,
    },
    Container: {
      height: 20,
    },
  },
};

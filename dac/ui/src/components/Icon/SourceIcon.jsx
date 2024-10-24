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
import { getSrcPath } from "#oss/utils/getIconPath";

export default class SourceIcon extends Component {
  static propTypes = {
    dremioIcon: PropTypes.string,
    style: PropTypes.object,
  };

  render() {
    const { style, dremioIcon } = this.props;
    const iconStyle = { ...styles.iconStyle, ...style };

    // ADX icon has a <linearGradient> property so we need to handle it through <img>
    // NETEZZA is a png and needs to be handled separately
    if (
      [
        "sources/ADX",
        "sources/NETEZZA",
        "sources/AWSGLUE",
        "sources/REDSHIFT",
        "sources/S3",
        "sources/MYSQL",
        "sources/POSTGRES",
        "sources/MSSQL",
        "sources/ORACLE",
        "sources/AMAZONELASTIC",
        "sources/GCS",
      ].includes(dremioIcon)
    ) {
      return (
        <img
          src={getSrcPath(
            // 'sources/ADX' get blocked by adblockers so need to fetch it as AZX
            dremioIcon === "sources/ADX" ? "sources/AZX" : dremioIcon,
            dremioIcon === "sources/NETEZZA" ? "png" : "svg",
          )}
          alt={dremioIcon.split("/")[1]}
          style={{
            ...iconStyle,
            ...(dremioIcon === "sources/ADX" && { height: 34, width: 34 }),
          }}
        />
      );
    }

    return <dremio-icon name={dremioIcon} style={iconStyle} />;
  }
}

const styles = {
  iconStyle: {
    margin: "0 20px 0 10px",
    width: 60,
    height: 60,
  },
};

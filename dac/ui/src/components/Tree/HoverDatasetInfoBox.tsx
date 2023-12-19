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
import {
  getIconAltTextByEntityIconType,
  getIconByEntityType,
  getSourceStatusIcon,
} from "utils/iconUtils";
import FontIcon from "@app/components/Icon/FontIcon";
// import { intl } from "@app/utils/intl";
import "./HoverDatasetInfoBox.less";

// Starring phase 2
type HoverDatasetInfoBoxProps = {
  classname: string;
  node: any;
  nodeStatus: any;
  fullpath: string;
};

const HoverDatasetInfoBox = ({
  classname,
  node,
  nodeStatus,
  fullpath,
}: HoverDatasetInfoBoxProps) => {
  const iconType =
    node.get("type") === "SOURCE"
      ? getSourceStatusIcon(nodeStatus)
      : getIconByEntityType(node.get("type"), !!node.get("versionContext"));
  const iconAltText = getIconAltTextByEntityIconType(iconType) || "";

  return (
    <div className={classname}>
      <div className={`${classname}__row`}>
        <FontIcon
          type={iconType}
          tooltip={iconAltText}
          hasCustomStyle
          iconStyle={iconStyle}
        />
        <span className={`${classname}__fileName`} title={node.get("name")}>
          {node.get("name")}
        </span>
        {/* When implemented, wrap with <IconButton> insstead of adding onClick and title here
        <dremio-icon
          className={`${classname}__linkIcon`}
          name="interface/external-link"
          alt={intl.formatMessage({ id: "Resource.Tree.Hover.Overlay" })}
          onClick={() => {}} // PHASE 2 STARRING
          title={intl.formatMessage({ id: "Resource.Tree.Hover.Overlay" })}
        /> */}
      </div>
      <span className={`${classname}__filePath`} title={fullpath}>
        {fullpath}
      </span>
    </div>
  );
};

const iconStyle = {
  display: "flex",
  alignItems: "center",
  position: "relative",
  left: "9%",
  top: "17%",
  marginRight: "8px",
  width: "32px",
  height: "32px",
};

export default HoverDatasetInfoBox;

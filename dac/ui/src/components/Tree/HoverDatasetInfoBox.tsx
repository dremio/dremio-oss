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
  getIconTypeByEntityTypeAndStatus,
} from "utils/iconUtils";
import FontIcon from "@app/components/Icon/FontIcon";
import { intl } from "@app/utils/intl";
import Art from "../Art";
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
  const iconType = getIconTypeByEntityTypeAndStatus(
    node.get("type"),
    nodeStatus
  );
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
        <Art
          className={`${classname}__linkIcon`}
          src="ExternalLinkIcon.svg"
          alt={intl.formatMessage({ id: "Resource.Tree.Hover.Overlay" })}
          onClick={() => {}} // PHASE 2 STARRING
          title={intl.formatMessage({ id: "Resource.Tree.Hover.Overlay" })}
        />
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

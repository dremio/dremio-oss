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
import FontIcon from "components/Icon/FontIcon";
import EllipsedText from "@app/components/EllipsedText";
import PartitionTransformationMenu from "@app/exports/components/PartitionTransformation/components/PartitionTransformationMenu/PartitionTransformationMenu";
import { injectIntl } from "react-intl";
import "@app/uiTheme/less/Acceleration/Acceleration.less";

class AccelerationGridSubCell extends Component {
  static propTypes = {
    isChecked: PropTypes.bool,
    onClick: PropTypes.func,
    isLastCell: PropTypes.bool,
    subValue: PropTypes.string,
    subValueAltText: PropTypes.string,
    onValueClick: PropTypes.func,
    currentRow: PropTypes.object,
    selectedField: PropTypes.object,
    granularity: PropTypes.object,
    setPartitionTransformation: PropTypes.func,
    hasPermission: PropTypes.bool,
    intl: PropTypes.any,
  };

  render() {
    const {
      isChecked,
      isLastCell,
      onClick,
      subValue,
      subValueAltText,
      onValueClick,
      currentRow,
      selectedField,
      granularity,
      setPartitionTransformation,
      hasPermission,
      intl: { formatMessage },
    } = this.props;
    const iconType = isChecked ? "OKSolid" : "MinusSimple";
    const disabledIconType = isChecked ? "OKSolidGrey" : "MinusSimple";
    const cellStyle = isLastCell
      ? "AccelerationGridSubCell__lastSubCell"
      : "AccelerationGridSubCell__subCell";
    const altText = subValueAltText ? `${subValueAltText}` : "";
    // if the cell is not checked, only show minus icon
    // otherwise show check round icon
    //   and if subValue is given, show it
    //   and if onValueClick is defined, show caret and handle click on subvalue with caret
    return (
      <div
        className={`${cellStyle} ${
          hasPermission ? "" : "--disabled hover-help"
        }`}
      >
        <EllipsedText
          className={"AccelerationGridSubCell__subVal"}
          title={hasPermission ? null : formatMessage({ id: "Read.Only" })}
          onClick={hasPermission ? onClick : null}
        >
          <FontIcon
            type={hasPermission ? iconType : disabledIconType}
            theme={hasPermission ? theme.iconTheme : theme.disabledTheme}
          />
        </EllipsedText>
        {isChecked && subValue && (
          <div
            title={altText}
            onClick={onValueClick}
            className={`${
              onValueClick
                ? "AccelerationGridSubCell__subValClickable"
                : "AccelerationGridSubCell__subVal"
            }`}
          >
            {subValue}
            {onValueClick && (
              <dremio-icon
                name="interface/caret-down"
                class="AccelerationGridSubCell__subValClickable--icon"
              />
            )}
          </div>
        )}
        {isChecked && currentRow && setPartitionTransformation && (
          <PartitionTransformationMenu
            currentRow={currentRow}
            selectedField={selectedField}
            granularity={granularity}
            setPartitionTransformation={setPartitionTransformation}
          />
        )}
      </div>
    );
  }
}

export default injectIntl(AccelerationGridSubCell);

const theme = {
  iconTheme: {
    Container: {
      cursor: "pointer",
      height: 24,
    },
  },
  caretTheme: {
    Container: {
      cursor: "pointer",
    },
  },
  disabledTheme: {
    Container: {
      cursor: "default",
    },
  },
};

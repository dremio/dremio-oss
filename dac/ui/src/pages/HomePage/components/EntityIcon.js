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
import { connect } from "react-redux";
import PropTypes from "prop-types";
import {
  getIconAltTextByEntityIconType,
  getIconTypeByEntityTypeAndStatus,
} from "utils/iconUtils";
import { ENTITY_TYPES } from "@app/constants/Constants";
import { getEntity } from "@app/selectors/resources";
import { getRootEntityTypeByIdV3 } from "@app/selectors/home";
import { Tooltip } from "dremio-ui-lib";

import FontIcon from "@app/components/Icon/FontIcon";

const mapStateToPropsForEntityIcon = (state, { entityId }) => {
  const type = getRootEntityTypeByIdV3(state, entityId);
  const props = {
    entityType: type,
    sourceStatus: null,
    sourceType: null,
  };

  if (type === ENTITY_TYPES.source) {
    // this work only for v2 api. We should think how to change this when we start migrate sources to v3 api
    const entity = getEntity(state, entityId, type);
    props.sourceStatus = entity.getIn(["state", "status"], null);
    props.sourceType = entity.get("type");
  }

  return props;
};

/**
 * Class that renders an entity icon based on entityId for home root entities (sources, spaces and home space).
 * If entityId is empty or entity with this id is not found in redux store, then icon is defaulted to home icon
 */
@connect(mapStateToPropsForEntityIcon)
export class EntityIcon extends PureComponent {
  static propTypes = {
    //public api entityId
    entityId: PropTypes.string,
    //connected
    entityType: PropTypes.oneOf([
      ENTITY_TYPES.home,
      ENTITY_TYPES.source,
      ENTITY_TYPES.space,
    ]).isRequired,
    sourceStatus: PropTypes.string, // available only for sources
    sourceType: PropTypes.string, // available only for sources
  };

  render() {
    const { entityType, sourceStatus, sourceType } = this.props;
    return (
      <PureEntityIcon
        entityType={entityType}
        sourceStatus={sourceStatus}
        sourceType={sourceType}
      />
    );
  }
}

export class PureEntityIcon extends PureComponent {
  static propTypes = {
    entityType: PropTypes.string.isRequired,
    sourceStatus: PropTypes.string,
    sourceType: PropTypes.string,
    style: PropTypes.object,
  };

  render() {
    const { entityType, sourceStatus, sourceType, style } = this.props;
    const iconType = getIconTypeByEntityTypeAndStatus(
      entityType,
      sourceStatus,
      sourceType
    );
    const iconAltText = getIconAltTextByEntityIconType(iconType) || "";

    return (
      <Tooltip title={iconAltText}>
        <span>
          <FontIcon type={iconType} theme={{ ...iconStyle, ...style }} />
        </span>
      </Tooltip>
    );
  }
}

const iconStyle = {
  Container: {
    height: 24,
    display: "inline-block",
    verticalAlign: "middle",
    marginRight: 5,
  },
};

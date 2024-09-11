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

import { getRootEntityNameV3 } from "@app/selectors/home";
import EllipsedText from "@app/components/EllipsedText";
import { Tooltip } from "dremio-ui-lib";

const mapStateToProps = (state, { entityId }) => ({
  name: getRootEntityNameV3(state, entityId),
});

@connect(mapStateToProps)
export class EntityName extends PureComponent {
  static propTypes = {
    //public api
    entityId: PropTypes.string,
    style: PropTypes.object,
    //connected
    name: PropTypes.string.isRequired,
  };

  render() {
    const { name, style } = this.props;

    return (
      <Tooltip title={name}>
        <EllipsedText style={style}>
          <span>{name}</span>
        </EllipsedText>
      </Tooltip>
    );
  }
}

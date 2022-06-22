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
import Immutable from "immutable";
import Radium from "radium";

import PropTypes from "prop-types";

import { title, contextCard, cardTitle } from "uiTheme/radium/rightContext";

class HomeContext extends PureComponent {
  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
  };

  static contextTypes = {
    username: PropTypes.string,
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { entity } = this.props;

    return (
      <div className="home-context">
        <h4 style={title}>About Home</h4>
        <div style={contextCard}>
          <div style={cardTitle}>Details</div>
          <div className="description">{entity.get("description")}</div>
        </div>
      </div>
    );
  }
}
export default Radium(HomeContext);

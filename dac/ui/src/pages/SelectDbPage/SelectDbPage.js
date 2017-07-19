/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Component, PropTypes } from 'react';
import { connect }   from 'react-redux';

import SelectedDb from './components/SelectedDb';
import SelectDb from './components/SelectDb';

class SelectDbPage extends Component {
  static propTypes = {
    routeParams: PropTypes.object
  }

  constructor(props) {
    super(props);
  }

  getComponentToDisplay() {
    const components = {
      selectdb: <SelectDb/>,
      dbtype: <SelectedDb type={''} />
    };
    return components[this.getCurrentPage()] || null;
  }

  getCurrentPage() {
    if (this.props.routeParams.dbtype) {
      return 'dbtype';
    }

    return 'selectdb';
  }

  render() {
    return (
      <div>
        {this.getComponentToDisplay()}
      </div>
    );
  }
}

SelectDbPage.propTypes = {};

function mapStateToProps() {
  return {};
}

export default connect(mapStateToProps, {})(SelectDbPage);

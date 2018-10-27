/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { Component } from 'react';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import UserNavigation from 'components/UserNavigation';
import MainHeader from 'components/MainHeader';

import './AdminPage.less'; // TODO to Vasyl, need to use Radium for each child component

@Radium
@pureRender
class AdminPageView extends Component {
  static propTypes = {
    sections: PropTypes.arrayOf(PropTypes.object),
    children: PropTypes.node,
    routeParams: PropTypes.object,
    location: PropTypes.object.isRequired,
    style: PropTypes.object
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { location, style, children, sections } = this.props;
    return (
      <div id='admin-page' style={style}>
        <MainHeader />
        <div className='page-content'>
          <UserNavigation
            sections={sections}
            location={location}
          />
          <div className='main-content' style={styles.mainContent}>
            {children}
          </div>
        </div>
      </div>
    );
  }
}


export default AdminPageView;

const styles = {
  mainContent: {
    width: '100%',
    padding: '0 10px'
  }
};

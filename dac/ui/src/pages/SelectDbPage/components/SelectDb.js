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
import { Link } from 'react-router';

import './SelectDb.less';

export default class SelectDb extends Component {
  static propTypes = {
    history: PropTypes.object.isRequired
  }

  constructor(props) {
    super(props);
    this.goToDbPage = this.goToDbPage.bind(this);
    this.getDbTiles = this.getDbTiles.bind(this);

    this.databases = [
      {name: 'mongoDb', className: 'mongoDb' },
      {name: 'elasticSearch', className: 'elasticSearch' },
      {name: 'phoenix', className: 'phoenix' },
      {name: 'amazonS3', className: 'amazonS3' },
      {name: 'mysql', className: 'mysql' },
      {name: 'hbase', className: 'hbase' },
      {name: 'oracle', className: 'oracle' },
      {name: 'hadoop', className: 'hadoop' },
      {name: 'hive', className: 'hive' }
    ];
  }

  getDbTiles() {
    return this.databases.map((item) => {
      return  (
        <Link to={'/setup/selectdb/' + item.name}
          key={item.className} className={item.className + ' tile'}/>
      );
    });
  }

  render() {
    return (
      <div>
        <h1 className='select-db-header'>Setup your first Database</h1>
        <div className='select-db'>
          {this.getDbTiles()}
        </div>
      </div>
    );
  }
}

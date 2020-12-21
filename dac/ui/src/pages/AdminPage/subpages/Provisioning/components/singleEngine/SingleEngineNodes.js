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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import StatefulTableViewer from '@app/components/StatefulTableViewer';
import SingleEngineNodesMixin from 'dyn-load/pages/AdminPage/subpages/Provisioning/components/singleEngine/SingleEngineNodesMixin';

@SingleEngineNodesMixin
export class SingleEngineNodes extends PureComponent {
  static propTypes = {
    engine: PropTypes.instanceOf(Immutable.Map),
    filterList: PropTypes.array
  };

  render() {
    const { engine} = this.props;
    const columns = this.getColumns(engine);
    const tableData = this.getTableData(engine);
    const renderEngineStatusBar = this.renderEngineStatusBar(engine);

    return (
      <>
        {renderEngineStatusBar}
        <div style={styles.base}>
          <StatefulTableViewer
            virtualized
            tableData={tableData}
            columns={columns}
          />
        </div>
      </>
    );
  }
}

const styles = {
  base: {
    height: '100%'
  }
};

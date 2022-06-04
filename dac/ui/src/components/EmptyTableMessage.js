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
import FontIcon from './Icon/FontIcon';

export default class EmptyTableMessage extends PureComponent {
  static propTypes = {
    noDataText: PropTypes.oneOfType([PropTypes.string, PropTypes.node]),
    tableViewer: PropTypes.object,
    isLoading: PropTypes.bool,
    loaderPosition: PropTypes.string
  };

  render() {
    const { noDataText, tableViewer, isLoading = false, loaderPosition = 'bot' } = this.props;

    return (
      <div className='empty-table' style={styles.emptyTable}>
        {tableViewer}
        <div className='empty-message'>
          <span>
            {noDataText}
          </span>
          {isLoading &&
          <div className={loaderPosition === 'bot' ? 'loader-bot' : 'loader-top'}>
            <FontIcon type='Loader spinner'/>
          </div>
          }
        </div>
      </div>
    );
  }
}

const styles = {
  emptyTable: {
    width: '100%'
  }
};

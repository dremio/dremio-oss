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
import { injectIntl } from 'react-intl';
import PropTypes from 'prop-types';
import SQL from '../SQL/SQL';
import DatasetGraph from './DatasetGraph';
import Dataset from './Dataset';
import './SQLTab.less';

const SQLTab = ({
  intl: {
    formatMessage
  },
  submittedSql,
  datasetGraph,
  algebricMatch,
  isContrast,
  onClick
}) => {
  const exceptionCheck = datasetGraph.toJS();
  return (
    <div className='sqlTab'>
      <SQL
        defaultContrast={isContrast}
        onClick={onClick}
        showContrast
        sqlString={submittedSql}
        sqlClass='sqlTab__SQLBody'
        title={formatMessage({ id: 'SubmittedSQL' })}
      />
      <span className='sqlTab__SQLGraphHeader'>
        {formatMessage({ id: 'DataSetGraph' })}
      </span>
      <div className='sqlTab__SQLQueryVisualizer'>
        {
          exceptionCheck.length && exceptionCheck[0].description ?
            <Dataset description={exceptionCheck[0].description} />
            :
            <DatasetGraph datasetGraph={datasetGraph} algebricMatch={algebricMatch} />
        }
      </div>
    </div>
  );
};
SQLTab.propTypes = {
  intl: PropTypes.object.isRequired,
  submittedSql: PropTypes.string,
  expandedSql: PropTypes.string,
  datasetGraph:PropTypes.object,
  isContrast: PropTypes.bool,
  onClick: PropTypes.func,
  algebricMatch: PropTypes.object
};
export default injectIntl(SQLTab);

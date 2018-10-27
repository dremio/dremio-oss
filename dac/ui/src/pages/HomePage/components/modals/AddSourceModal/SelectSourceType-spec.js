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
import { shallow } from 'enzyme';

import SelectSourceType from './SelectSourceType';

const SOURCE_LIST = [
  {'label': 'Amazon Redshift', 'sourceType': 'REDSHIFT', 'tags': ['beta']},
  {'label': 'Amazon S3', 'sourceType': 'S3', 'tags': ['beta']},
  {'label': 'Elasticsearch', 'sourceType': 'ELASTIC'},
  {'label': 'HBase', 'sourceType': 'HBASE', 'tags': ['beta']},
  {'label': 'HDFS', 'sourceType': 'HDFS'},
  {'label': 'Hive', 'sourceType': 'HIVE'},
  {'label': 'MapR-FS', 'sourceType': 'MAPRFS'},
  {'label': 'Microsoft SQL Server', 'sourceType': 'MSSQL'},
  {'label': 'MongoDB', 'sourceType': 'MONGO', 'tags': ['beta']},
  {'label': 'MySQL', 'sourceType': 'MYSQL'},
  {'label': 'NAS', 'sourceType': 'NAS'},
  {'label': 'Oracle', 'sourceType': 'ORACLE'},
  {'label': 'PostgreSQL', 'sourceType': 'POSTGRES', 'tags': ['beta']},
  {'label': 'Azure Data Lake Store', 'sourceType': 'ADL', 'tags': ['beta']},
  {'label': 'IBM DB2', 'sourceType': 'DB2'}
];


describe('SelectSourceType', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {
      onSelectSource: sinon.spy(),
      onAddSampleSource: sinon.spy(),
      sourceTypes: SOURCE_LIST
    };
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<SelectSourceType {...commonProps}/>);
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<SelectSourceType {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render SelectConnectionButton for each sourceType', () => {
    expect(wrapper.find('SelectConnectionButton')).to.have.length(SOURCE_LIST.length + 1);
  });
});

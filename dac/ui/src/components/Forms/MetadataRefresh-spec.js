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
import { shallow } from 'enzyme';
import Immutable from 'immutable';

import MetadataRefresh from './MetadataRefresh';

const hour = 60 * 60 * 1000;
const day = 24 * hour;

describe('MetadataRefresh', () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      fields: {
        metadataPolicy: {}
      }
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<MetadataRefresh {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#mapToFormFields', () => {
    it('should return default values if source is not defined', () => {
      expect(MetadataRefresh.mapToFormFields()).to.be.eql(MetadataRefresh.defaultFormValues().metadataPolicy);
    });

    it('should convert metadataPolicy values for form to appropriate format', () => {
      const source = Immutable.fromJS({
        metadataPolicy: {
          namesRefreshMillis: 40 * hour,
          datasetDefinitionTTLMillis: 30 * day,
          authTTLMillis: 7 * day,
          updateMode: 'PREFETCH'
        }
      });
      expect(MetadataRefresh.mapToFormFields(source)).to.be.eql({
        authTTLMillis: {
          duration: 7,
          unit: 'DAYS'
        },
        datasetDefinitionTTLMillis: {
          duration: 5,
          unit: 'WEEKS'
        },
        namesRefreshMillis: {
          duration: 2,
          unit: 'DAYS'
        },
        updateMode: 'PREFETCH'
      });
    });
  });

  describe('#normalizeValues', () => {
    it('should convert metadataPolicy values according to payload format', () => {
      const values = {
        metadataPolicy: {
          authTTLMillis: {
            duration: 7,
            unit: 'DAYS'
          },
          datasetDefinitionTTLMillis: {
            duration: 5,
            unit: 'WEEKS'
          },
          namesRefreshMillis: {
            duration: 2,
            unit: 'DAYS'
          },
          updateMode: 'PREFETCH'
        }
      };
      expect(MetadataRefresh.normalizeValues(values)).to.be.eql({
        namesRefreshMillis: 2 * day,
        datasetDefinitionTTLMillis: 5 * 7 * day,
        authTTLMillis: 7 * day,
        updateMode: 'PREFETCH'
      });
    });
  });
});

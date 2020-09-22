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
import { shallow } from 'enzyme';

import FormSection from './FormSection';

describe('FormSection', () => {
  const minimalProps = {
    sectionConfig: {
      getSections() {
        return [];
      },

      getConfig() {
        return [];
      },

      getDirectElements() {
        return [];
      }
    }
  };

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<FormSection {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render no children if visibilityControl is false', () => {
    const instance = shallow(<FormSection {...minimalProps}/>).instance();
    const render = instance.renderElements({
      getConfig() {
        return {};
      },

      getDirectElements() {
        return [
          {
            getConfig() {
              return {
                visibilityControl: {
                  config: 'allowFileUploads',
                  showCondition: false
                }
              };
            }
          }
        ];
      }
    });

    expect(shallow(render).children()).to.have.length(0);
  });

  it('should render children if visibilityControl is true', () => {
    const instance = shallow(<FormSection {...minimalProps}/>).instance();
    const render = instance.renderElements({
      getConfig() {
        return {};
      },

      getDirectElements() {
        return [
          {
            getConfig() {
              return {
                visibilityControl: {
                  config: 'allowFileUploads',
                  showCondition: true
                }
              };
            }
          }
        ];
      }
    });

    expect(shallow(render).children()).to.have.length(1);
  });
});

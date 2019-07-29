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
import AccelerationController from 'components/Acceleration/AccelerationController';

import FormUnsavedRouteLeave from 'components/Forms/FormUnsavedRouteLeave';


export class Reflections extends PureComponent {

  static propTypes = {
    datasetId: PropTypes.string,

    //from FormUnsavedRouteLeave
    updateFormDirtyState: PropTypes.func,
    router: PropTypes.object
  };

  render() {
    const { datasetId, updateFormDirtyState } = this.props;
    return (
      <AccelerationController
        isModal={false}
        updateFormDirtyState={updateFormDirtyState}
        onDone={() => {}}
        datasetId={datasetId}
      />
    );
  }

}

export default FormUnsavedRouteLeave(Reflections);

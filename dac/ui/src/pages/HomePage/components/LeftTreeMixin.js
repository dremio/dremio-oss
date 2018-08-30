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
import PropTypes from 'prop-types';
import Immutable from 'immutable';

export const mixin = (input) => {
  //do nothing
};

export const propTypes = {
  className: PropTypes.string,
  spaces: PropTypes.instanceOf(Immutable.List).isRequired,
  sources: PropTypes.instanceOf(Immutable.List).isRequired,
  spacesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
  sourcesViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
  toggleSpacePin: PropTypes.func,
  toggleSourcePin: PropTypes.func,
  createSampleSource: PropTypes.func.isRequired,
  intl: PropTypes.object.isRequired
};

export const mapStateToProps = null;

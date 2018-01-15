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
import PropTypes from 'prop-types';

export const RESERVED = [
  'support.email.addr',
  'support.email.jobs.subject'
];

const InternalSupportEmail = (props) => {
  return (
    <div>
      <h3 style={{margin:'15px 0 0'}}>{la('Internal Support Email')}</h3>
      <div style={props.descriptionStyle}>
        {la('Note: Users will see changes when they next reload.')}
      </div>
      {props.renderSettings('support.email.addr')}
      {props.renderSettings('support.email.jobs.subject')}
    </div>
  );
};

InternalSupportEmail.propTypes = {
  descriptionStyle: PropTypes.object,
  renderSettings: PropTypes.func
};

export default InternalSupportEmail;

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
import PropTypes from 'prop-types';
import Art from '@app/components/Art';

export const getReflectionIcon = () => {
  return {
    columnData: undefined,
    dataKey: <ReflectionIcon isAcceleration />,
    disableSort: true,
    label: <ReflectionIcon isAcceleration />,
    sortBy: undefined,
    sortDirection: undefined
  };
};

const ReflectionIcon = ({ isAcceleration }) => {

  return <span className='jobsContent-dataset__accelerationIconWrapper'>
    {
      isAcceleration && <Art
        src='Reflection.svg'
        alt='Query was accelerated Icon'
        title='Query was accelerated'
        className='jobsContent-dataset__accelerationIcon'
      />
    }
  </span>;

};

ReflectionIcon.propTypes = {
  isAcceleration: PropTypes.bool
};

export default ReflectionIcon;

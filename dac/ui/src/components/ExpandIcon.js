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
import { PropTypes } from 'react';
import FontIcon from 'components/Icon/FontIcon';

const ARROW_DOWN = 'Arrow-Down-Small';

const ExpandIcon = ({ expanded }) => {
  const topArrowStyle = expanded ? {
    ...styles.transform,
    bottom: 1
  } : {};
  const bottomArrowStyle = !expanded ? {
    bottom: -7
  } : styles.transform;
  return (
    <div style={styles.wrapper}>
      <FontIcon type={ARROW_DOWN} style={{ ...styles.base, ...topArrowStyle }}/>
      <FontIcon type={ARROW_DOWN} style={{ ...styles.base, ...bottomArrowStyle }}/>
    </div>
  );
};

ExpandIcon.propTypes = {
  expanded: PropTypes.bool
};

const styles = {
  wrapper: {
    height: 24,
    position: 'relative'
  },
  base: {
    position: 'absolute'
  },
  transform: {
    transform: 'rotate(180deg)'
  }
};

export default ExpandIcon;

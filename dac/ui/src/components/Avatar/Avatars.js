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
import { Component } from 'react';
import Radium from 'radium';

import FontIcon from 'components/Icon/FontIcon';

@Radium
export default class Avatars extends Component {

  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div className='avatars-wrapper' style={styles.main}>
        <FontIcon type='Bear' theme={styles.avatar} />
        <FontIcon type='Bird' theme={styles.avatar} />
        <FontIcon type='BlueDonkey' theme={styles.avatar} />
        <FontIcon type='Boar' theme={styles.avatar} />
        <FontIcon type='BullBrown' theme={styles.avatar} />
        <FontIcon type='BullWhite' theme={styles.avatar} />
        <FontIcon type='CatBlack' theme={styles.avatar} />
        <FontIcon type='CatGold' theme={styles.avatar} />
        <FontIcon type='CatWhite' theme={styles.avatar} />
        <FontIcon type='Cheetah' theme={styles.avatar} />
        <FontIcon type='ChickBrown' theme={styles.avatar} />
        <FontIcon type='ChickHatch' theme={styles.avatar} />
        <FontIcon type='Chicken' theme={styles.avatar} />
        <FontIcon type='Cougar' theme={styles.avatar} />
        <FontIcon type='Cow' theme={styles.avatar} />
        <FontIcon type='CowBrown' theme={styles.avatar} />
        <FontIcon type='Cub' theme={styles.avatar} />
        <FontIcon type='Deer' theme={styles.avatar} />
        <FontIcon type='Elephant' theme={styles.avatar} />
        <FontIcon type='Fox' theme={styles.avatar} />
        <FontIcon type='Gazelle' theme={styles.avatar} />
        <FontIcon type='Hen' theme={styles.avatar} />
        <FontIcon type='Horse' theme={styles.avatar} />
        <FontIcon type='Horse-2' theme={styles.avatar} />
        <FontIcon type='Jaguar' theme={styles.avatar} />
        <FontIcon type='Koala' theme={styles.avatar} />
        <FontIcon type='Lion' theme={styles.avatar} />
        <FontIcon type='Llama' theme={styles.avatar} />
        <FontIcon type='Lynx' theme={styles.avatar} />
        <FontIcon type='MonkeyBlack' theme={styles.avatar} />
        <FontIcon type='MonkeyBrown' theme={styles.avatar} />
        <FontIcon type='MonkeyGold' theme={styles.avatar} />
        <FontIcon type='MonkeyWhite' theme={styles.avatar} />
        <FontIcon type='MountainGoat' theme={styles.avatar} />
        <FontIcon type='OwlGrey' theme={styles.avatar} />
        <FontIcon type='OwlTan' theme={styles.avatar} />
        <FontIcon type='Panda' theme={styles.avatar} />
        <FontIcon type='Penguin' theme={styles.avatar} />
        <FontIcon type='Pig' theme={styles.avatar} />
        <FontIcon type='PolarBear' theme={styles.avatar} />
        <FontIcon type='Puma' theme={styles.avatar} />
        <FontIcon type='Rabbit' theme={styles.avatar} />
        <FontIcon type='Racoon' theme={styles.avatar} />
        <FontIcon type='Ram' theme={styles.avatar} />
        <FontIcon type='Sheep' theme={styles.avatar} />
        <FontIcon type='SnowLeopard' theme={styles.avatar} />
        <FontIcon type='Tapir' theme={styles.avatar} />
        <FontIcon type='Tiger' theme={styles.avatar} />
        <FontIcon type='Turkey' theme={styles.avatar} />
        <FontIcon type='Zebra' theme={styles.avatar} />
      </div>
    );
  }
}

const styles = {
  main: {
    width: '100%',
    margin: '0 auto'
  },
  avatar: {
    'Icon': {
      width: 40,
      height: 40
    },
    'Container': {
      height: 40,
      width: 40,
      margin: 5
    }
  }
};

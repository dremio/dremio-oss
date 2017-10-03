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
import { Component, PropTypes } from 'react';
import FieldList, {RemoveButton} from 'components/Fields/FieldList';
import FieldWithError from 'components/Fields/FieldWithError';
import TextField from 'components/Fields/TextField';
import SimpleButton from 'components/Buttons/SimpleButton';

import { PALE_NAVY } from 'uiTheme/radium/colors';
import { FLEX_NOWRAP_ROW_BETWEEN_CENTER } from 'uiTheme/radium/flexStyle';
import { body } from 'uiTheme/radium/typography';
import EllipsedText from 'components/EllipsedText';

const MAX_WIDTH = 262;

export class ValueItem extends Component {
  static propTypes = {
    item: PropTypes.object,
    onRemove: PropTypes.func
  };

  constructor(props) {
    super(props);
    this.showTooltip = this.showTooltip.bind(this);
    this.state = { textWidth: null };
  }

  componentDidMount() {
    if (this.greaterThanMaxWidth()) {
      this.showTooltip(this.refs.target.clientWidth);
    }
  }

  greaterThanMaxWidth() {
    return this.refs.target.clientWidth >= MAX_WIDTH;
  }

  showTooltip = textWidth => this.setState({ textWidth });

  render() {
    const { item, onRemove } = this.props;
    return (
      <div style={styles.item}>
        <EllipsedText style={styles.itemText} text={item.value} ref='target'/>
        {onRemove && <RemoveButton style={{margin: '2px 10px 0 0'}} onClick={onRemove}/>}
      </div>
    );
  }
}


export default class ValueList extends Component {

  static propTypes = {
    fieldList: PropTypes.array,
    fieldLabel: PropTypes.string,
    emptyLabel: PropTypes.string
  };

  static defaultProps = {
    fieldLabel: 'New Value'
  };

  constructor(props) {
    super(props);
    this.state = {inputValue: '', inputError: null};

    this.onInputChange = this.onInputChange.bind(this);
    this.addItem = this.addItem.bind(this);
  }

  //
  // Handlers
  //

  onInputChange(e) {
    this.setState({inputValue: e.target.value, inputError: null});
  }

  addItem(e) {
    e.preventDefault();
    const {fieldList} = this.props;
    const {inputValue} = this.state;

    if (!inputValue) {
      this.setState({inputError: 'Enter a value'});
    } else if (fieldList.map((x) => x.value).indexOf(inputValue) !== -1) {
      this.setState({inputError: 'Already added'});
    } else {
      fieldList.addField(inputValue);
      this.setState({inputValue: '', inputError: null});
    }
  }

  render() {
    const {fieldList, emptyLabel} = this.props;
    return (
      <div>
        <FieldList
          style={{margin: '10px 0'}}
          items={fieldList}
          itemHeight={28}
          getKey={item => item.value}
          emptyLabel={emptyLabel}>
          <ValueItem/>
        </FieldList>

        <div>
          <FieldWithError error={this.state.inputError} touched style={{display: 'inline-block'}}>
            <TextField
              type='text'
              value={this.state.inputValue}
              onChange={this.onInputChange}
              error={this.state.inputError}
              touched />
          </FieldWithError>
          <SimpleButton
            buttonStyle='secondary'
            onClick={this.addItem}>
            {la('Add')}
          </SimpleButton>
        </div>
      </div>
    );
  }
}

const styles = {
  item: {
    ...FLEX_NOWRAP_ROW_BETWEEN_CENTER,
    width: 310,
    backgroundColor: PALE_NAVY,
    borderRadius: 30,
    margin: '8px 0'
  },
  itemText: {
    ...body,
    marginLeft: 10
  },
  addButton: {
    padding: '7px 10px'
  }
};

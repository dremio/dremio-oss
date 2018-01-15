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
import { Component } from 'react';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import { CANCEL } from 'components/Buttons/ButtonTypes';
import Button from 'components/Buttons/Button';
import ExistingForm from 'components/formsForAddData/ExistingForm';
import ModalFooter from 'components/Modals/components/ModalFooter';
import { connectComplexForm, InnerComplexForm } from 'components/Forms/connectComplexForm';
import AddDatasetFromExistingMenu from 'components/Menus/ExplorePage/AddDatasetFromExistingMenu';
import DropdownButton from 'components/Buttons/DropdownButton';
import './AddDataset.less';

@Radium
@PureRender
class AddDataset extends Component {
  static propTypes = {
    hide: PropTypes.func.isRequired,
    items: PropTypes.array.isRequired,
    hideDropdown: PropTypes.func.isRequired,
    createDataset: PropTypes.func.isRequired,
    updateName: PropTypes.func.isRequired,
    changeSelectedNode: PropTypes.func.isRequired,
    nameDataset: PropTypes.string
  };

  constructor(props) {
    super(props);
    this.sendData = this.sendData.bind(this);
  }

  componentDidMount() {
    this.clickListener = (e) => {
      if (!e.target.className.match('dropdown-item') &&
          !e.target.className.match('dropdown-button') &&
          !e.target.className.match('fa-angle-down')) {
        this.props.hideDropdown();
      }
    };
    $(document).on('click', this.clickListener);
  }

  componentWillUnmount() {
    $(document).off('click', this.clickListener);
  }

  sendData(item) {
    if (this.props.createDataset && this.props.nameDataset) {
      this.props.createDataset(this.refs.form, item);
    }
  }

  render() {
    return (
      <div className='add-dataset-form'>
        <InnerComplexForm
          {...this.props}
          ref='form'
          onSubmit={this.sendData}>
          <div className='add-dataset' onClick={this.preventClick}>
            <div className='dataset-content'>
              <ExistingForm
                updateName={this.props.updateName}
                changeSelectedNode={this.props.changeSelectedNode}
                nameDataset={this.props.nameDataset}/>
            </div>
            <ModalFooter>
              <Button type={CANCEL} onClick={this.props.hide}/>
              <DropdownButton
                action={this.sendData}
                type='primary'
                defaultValue={{label: 'Add', name: 'add'}}
                menu={<AddDatasetFromExistingMenu/>}
            />
            </ModalFooter>
          </div>
        </InnerComplexForm>
      </div>
    );
  }
}

export default connectComplexForm({
  form: 'add-dataset',
  fields: ['data']
}, [], null, null)(AddDataset);

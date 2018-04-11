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
import { Link } from 'react-router';
import { connect }   from 'react-redux';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import { PALE_GREY } from 'uiTheme/radium/colors';
import { formLabel } from 'uiTheme/radium/typography';
import { sectionTitle, description, formRow } from 'uiTheme/radium/forms';
import { setConnectionBiTool } from 'actions/account';
import { Select } from 'components/Fields';
import './Business.less';

// todo: loc

@pureRender
@Radium
class Business extends Component {
  static propTypes = {
    setConnectionBiTool: PropTypes.func
  }

  constructor(props) {
    super(props);
    this.state = {
      restVal: 'Select…'
    };
    this.BiOption = Immutable.List([
      Immutable.Map({ value: 'tableau', label: 'Tableau' }),
      Immutable.Map({ value: 'val1', label: 'Some label 1' }),
      Immutable.Map({ value: 'val2', label: 'Some label 2' })
    ]);
  }

  setConnectBITool(value) {
    this.props.setConnectionBiTool(value);
  }

  restChange(val) {
    this.setState({
      restVal: val
    });
  }

  render() {
    return (
      <div id='account-info'>
        <h2 style={[styles.header]}>Business Intelligence (BI)</h2>
        <div className='description' style={description}>
          The <Link to='#'>Dremio Connector</Link> enables BI tools to connect to a Dremio Cluster..
        </div>
        <div className='pageitems'>
          <h2 style={[styles.setTopIndent('25px'), sectionTitle]}>Automatically Connect to a BI Tool</h2>
          <div className='description' style={description}>
            Establish a connection from the BI tool to the Dremio cluster:
          </div>
          <div style={formRow}>
            <label style={formLabel}>{la('BI Tool')}</label>
          </div>
          <div style={formRow}>
            <Select
              style={styles.select}
              value={this.state.restVal}
              items={this.BiOption.toJS()}
              onChange={this.restChange.bind(this)}
            />
            <div style={[styles.button]} onClick={this.setConnectBITool.bind(this, this.state.restVal)}>
              {la('Connect')}
            </div>
          </div>
          <div className='description' style={[styles.clear, description]}>
            Having problems? Make sure you have installed the <Link to='#'>Dremio Connector</Link>.
          </div>

          <h2 style={[styles.setTopIndent('45px'), sectionTitle]}>Manually Connect to a BI Tool</h2>
          <div className='description' style={description}>
            Want to use a client that’s not listed? Dremio works with any client that supports ODBC or JDBC.
            Follow the <Link to='#'>step-by-step instructions</Link> and copy the appropriate connection string:
          </div>
          <div style={formRow}>
            <h3 style={[formLabel, styles.listTitle]}>JDBC:</h3>
            <div>
              jdbc://foo/bar/baz
            </div>
          </div>
          <div style={formRow}>
            <h3 style={[formLabel, styles.listTitle]}>ODBC:</h3>
            <div>Driver=(...); Server=...; Port=...</div>
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  button: {
    'backgroundColor': '#F5F5F5',
    'outline': 'none',
    'border': '1px solid #ddd',
    'boxShadow': '0 0 2px #ddd',
    'borderRadius': '3px',
    'position': 'relative',
    'fontSize': '13px',
    'cursor': 'pointer',
    'fontWeight': 'bold',
    'textAlign': 'center',
    'padding': '6px 10px',
    'float': 'left',
    ':hover': {
      'backgroundColor': '#bbb'
    }
  },
  select: {
    height: 31,
    marginRight: 5,
    marginLeft: 0,
    float: 'left',
    width: 310
  },
  clear: {
    'clear': 'both'
  },
  listTitle: {
    'fontWeight': 'bold'
  },
  header: {
    borderBottom: `2px solid ${PALE_GREY}`,
    marginBottom: 10,
    paddingBottom: 10
  },
  setTopIndent: (top) => {
    return {
      'marginTop': top
    };
  }
};

export default connect(null, {
  setConnectionBiTool
})(Business);

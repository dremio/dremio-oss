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
import { CALL_API } from 'redux-api-middleware';

import crudFactory from './crudFactory';


describe('crudFactory', () => {

  let actions;
  let meta;
  beforeEach(() => {
    actions = crudFactory('ent');
    meta = {bar: true};
  });

  it('POST', () => {
    const req = actions.post({a: 1}, meta)[CALL_API];
    expect(req.method).to.eql('POST');
    expect(req.endpoint.endsWith('/ents/')).to.be.true;
    expect(req.headers).to.eql({'Content-Type': 'application/json'});
    expect(req.types.map(e => e.meta)).to.eql(new Array(3).fill(meta));
    expect(req.body).to.eql(JSON.stringify({a: 1}));
  });

  it('GET', () => {
    const req = actions.get('id', meta)[CALL_API];
    expect(req.method).to.eql('GET');
    expect(req.endpoint.endsWith('/ents/id')).to.be.true;
    expect(req.headers).to.eql({'Content-Type': 'application/json'});
    expect(req.types.map(e => e.meta)).to.eql(new Array(3).fill(meta));
    expect(req.body).to.be.undefined;
  });

  it('PUT', () => {
    const req = actions.put({a: 1, id: 'id'}, meta)[CALL_API];
    expect(req.method).to.eql('PUT');
    expect(req.endpoint.endsWith('/ents/id')).to.be.true;
    expect(req.headers).to.eql({'Content-Type': 'application/json'});
    expect(req.types.map(e => e.meta)).to.eql(new Array(3).fill(meta));
    expect(req.body).to.eql(JSON.stringify({a: 1, id: 'id'}));
  });

  it('DELETE', () => {
    const req = actions.delete({id: 'id'}, meta)[CALL_API];
    expect(req.method).to.eql('DELETE');
    expect(req.endpoint.endsWith('/ents/id')).to.be.true;
    expect(req.headers).to.eql({'Content-Type': 'application/json'});
    expect(req.types.map(e => e.meta)).to.eql(
      [meta, {...meta, success: true, entityRemovePaths: [['ent', 'id']]}, meta]
    );
    expect(req.body).to.be.undefined;
  });

  it('DELETE with version', () => {
    const req = actions.delete({id: 'id', version: 0}, meta)[CALL_API];
    expect(req.method).to.eql('DELETE');
    expect(req.endpoint.endsWith('/ents/id?version=0')).to.be.true;
    expect(req.headers).to.eql({'Content-Type': 'application/json'});
    expect(req.types.map(e => e.meta)).to.eql(
      [meta, {...meta, success: true, entityRemovePaths: [['ent', 'id']]}, meta]
    );
    expect(req.body).to.be.undefined;
  });

  it('GET all', () => {
    const req = actions.getAll(meta)[CALL_API];
    expect(req.method).to.eql('GET');
    expect(req.endpoint.endsWith('/ents/')).to.be.true;
    expect(req.headers).to.eql({'Content-Type': 'application/json'});
    expect(req.types.map(e => e.meta)).to.eql([meta, {...meta, entityClears: ['ent']}, meta]);
    expect(req.body).to.be.undefined;
  });

  it('useLegacyPluralization = true', () => {
    actions = crudFactory('ent', {useLegacyPluralization: true});
    const req = actions.get('id', meta)[CALL_API];
    expect(req.endpoint.endsWith('/ent/id')).to.be.true;
  });

});

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

import DB2Form from 'dyn-load/components/sourceForms/DB2Form';
import ElasticForm from './ElasticForm';
import HBaseForm from './HBaseForm';
import HDFSForm from './HDFSForm';
import MapRFSForm from './MapRFSForm';
import MongoDBForm from './MongoDBForm';
import MySQLForm from './MySQLForm';
import OracleForm from './OracleForm';
import NASForm from './NASForm';
import PostgreSQLForm from './PostgreSQLForm';
import S3Form from './S3Form';
import SQLServerForm from './SQLServerForm';
import HiveForm from './HiveForm';
import RedshiftForm from './RedshiftForm';
import ADLForm from './ADLForm';

const sourceForms = {
  ElasticForm,
  HBaseForm,
  HDFSForm,
  MapRFSForm,
  MongoDBForm,
  MySQLForm,
  OracleForm,
  NASForm,
  PostgreSQLForm,
  S3Form,
  SQLServerForm,
  HiveForm,
  DB2Form,
  RedshiftForm,
  ADLForm
};

// only include available source forms
for (const [key, sourceForm] of Object.entries(sourceForms)) {
  if (!sourceForm) {
    delete sourceForms[key];
  }
}

export default sourceForms;

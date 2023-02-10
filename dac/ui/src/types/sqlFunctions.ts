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

// This was originally generated from swagger, had to make some changes for TS and API updates

/**
 *
 * @export
 * @interface FunctionSignature
 */
export interface FunctionSignature {
  /**
   *
   * @type {string}
   * @memberof FunctionSignature
   */
  returnType?: FunctionSignatureReturnTypeEnum;
  /**
   *
   * @type {Array<Parameter>}
   * @memberof FunctionSignature
   */
  parameters?: Array<Parameter>;
  /**
   *
   * @type {string}
   * @memberof FunctionSignature
   */
  description?: string;
  /**
   *
   * @type {Array<SampleCode>}
   * @memberof FunctionSignature
   */
  sampleCodes?: Array<SampleCode>;
  /**
   *
   * @type {string}
   * @memberof FunctionSignature
   */
  snippetOverride?: string;
}

/**
 * @export
 * @enum {string}
 */
export enum FunctionSignatureReturnTypeEnum {
  ANY = <any>"ANY",
  BOOLEAN = <any>"BOOLEAN",
  NUMERIC = <any>"NUMERIC",
  STRING = <any>"STRING",
  DATEANDTIME = <any>"DATEANDTIME",
  LIST = <any>"LIST",
  STRUCT = <any>"STRUCT",
  BYTES = <any>"BYTES",
  CHARACTERS = <any>"CHARACTERS",
  FLOAT = <any>"FLOAT",
  DECIMAL = <any>"DECIMAL",
  DOUBLE = <any>"DOUBLE",
  INT = <any>"INT",
  BIGINT = <any>"BIGINT",
  DATE = <any>"DATE",
  TIME = <any>"TIME",
  TIMESTAMP = <any>"TIMESTAMP",
}

/**
 *
 * @export
 * @interface ModelFunction
 */
export interface ModelFunction {
  /**
   *
   * @type {string}
   * @memberof ModelFunction
   */
  name: string;
  /**
   *
   * @type {Array<FunctionSignature>}
   * @memberof ModelFunction
   */
  signatures?: Array<FunctionSignature>;
  /**
   *
   * @type {string}
   * @memberof ModelFunction
   */
  dremioVersion?: string;
  /**
   *
   * @type {Array<string>}
   * @memberof ModelFunction
   */
  functionCategories?: Array<ModelFunctionFunctionCategoriesEnum>;
  /**
   *
   * @type {string}
   * @memberof ModelFunction
   */
  description?: string;
}

/**
 * @export
 * @enum {string}
 */
export enum ModelFunctionFunctionCategoriesEnum {
  AGGREGATE = <any>"AGGREGATE",
  BINARY = <any>"BINARY",
  BOOLEAN = <any>"BOOLEAN",
  BITWISE = <any>"BITWISE",
  CHARACTER = <any>"CHARACTER",
  CONDITIONAL = <any>"CONDITIONAL",
  CONTEXT = <any>"CONTEXT",
  CONVERSION = <any>"CONVERSION",
  DATETIME = <any>"DATETIME",
  DATETYPE = <any>"DATETYPE",
  DIRECTORY = <any>"DIRECTORY",
  GEOSPATIAL = <any>"GEOSPATIAL",
  MATH = <any>"MATH",
  WINDOW = <any>"WINDOW",
}

/**
 *
 * @export
 * @interface Parameter
 */
export interface Parameter {
  /**
   *
   * @type {string}
   * @memberof Parameter
   */
  kind?: ParameterKindEnum;
  /**
   *
   * @type {string}
   * @memberof Parameter
   */
  type?: ParameterTypeEnum;
  /**
   *
   * @type {string}
   * @memberof Parameter
   */
  name?: string;
  /**
   *
   * @type {string}
   * @memberof Parameter
   */
  description?: string;
  /**
   *
   * @type {string}
   * @memberof Parameter
   */
  format?: string;
}

/**
 * @export
 * @enum {string}
 */
export enum ParameterKindEnum {
  REGULAR = <any>"REGULAR",
  VARARG = <any>"VARARG",
  OPTIONAL = <any>"OPTIONAL",
}

/**
 * @export
 * @enum {string}
 */
export enum ParameterTypeEnum {
  ANY = <any>"ANY",
  BOOLEAN = <any>"BOOLEAN",
  NUMERIC = <any>"NUMERIC",
  STRING = <any>"STRING",
  DATEANDTIME = <any>"DATEANDTIME",
  LIST = <any>"LIST",
  STRUCT = <any>"STRUCT",
  BYTES = <any>"BYTES",
  CHARACTERS = <any>"CHARACTERS",
  FLOAT = <any>"FLOAT",
  DECIMAL = <any>"DECIMAL",
  DOUBLE = <any>"DOUBLE",
  INT = <any>"INT",
  BIGINT = <any>"BIGINT",
  DATE = <any>"DATE",
  TIME = <any>"TIME",
  TIMESTAMP = <any>"TIMESTAMP",
}

/**
 *
 * @export
 * @interface SampleCode
 */
export interface SampleCode {
  /**
   *
   * @type {string}
   * @memberof SampleCode
   */
  call?: string;
  /**
   *
   * @type {string}
   * @memberof SampleCode
   */
  result?: string;
}

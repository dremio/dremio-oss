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
package com.dremio.exec.tablefunctions;

import org.apache.calcite.runtime.CalciteResource;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.runtime.Resources.BaseMessage;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.validate.SqlValidatorException;

/** Dremio's Calcite Resources */
public interface DremioCalciteResource extends CalciteResource {
  DremioCalciteResource DREMIO_CALCITE_RESOURCE = Resources.create(DremioCalciteResource.class);

  /**
   * A Calcite exception that reports an external query metadata retrieval error.
   *
   * @param cause the cause of the exception.
   * @return A SqlValidationException capturing the External Query metadata retrieval error.
   */
  @BaseMessage(
      "Resulted in error when attempting to retrieve metadata for External Query. Caused by ''{0}''")
  ExInst<SqlValidatorException> externalQueryMetadataRetrievalError(Throwable cause);

  /**
   * A Calcite exception that reports an unsupported operation error when external query is not
   * supported on the source.
   *
   * @param errorMessage the reason external query is not supported.
   * @return A SqlValidationException capturing the unsupported error supplemented with a suggested
   *     followup action.
   */
  @BaseMessage("{0}")
  ExInst<SqlValidatorException> externalQueryNotSupportedError(String errorMessage);

  /**
   * A Calcite exception that reports an invalid external query for a given source.
   *
   * @return A SqlValidationException capturing the source name.
   */
  @BaseMessage("Invalid External Query statement on source <{0}>")
  ExInst<SqlValidatorException> externalQueryInvalidError(String sourceName);

  @BaseMessage(
      "Number of target columns ({0,number}) does not equal number of source items ({1,number})")
  ExInst<SqlValidatorException> unmatchColumn(int a0, int a1);
}

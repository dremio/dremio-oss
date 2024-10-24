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
package com.dremio.sabot.op.project;

import com.dremio.common.expression.BasePath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.VectorResolver;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.inject.Named;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.TransferPair;

public abstract class ProjectorTemplate implements Projector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectorTemplate.class);

  private ImmutableList<TransferPair> transfers;
  private SelectionVector2 vector2;
  private SelectionVectorMode svMode;
  private boolean trackRecordLevelErrors = false;
  private VarCharVector errorVector = null;

  public ProjectorTemplate() throws SchemaChangeException {}

  @Override
  public final void projectRecords(final int recordCount) {
    switch (svMode) {
      case TWO_BYTE:
        if (!trackRecordLevelErrors) {
          for (int i = 0; i < recordCount; i++) {
            doEval(vector2.getIndex(i), i);
          }
        } else {
          for (int i = 0; i < recordCount; i++) {
            try {
              doEval(vector2.getIndex(i), i);
            } catch (Exception exception) {
              errorVector.setSafe(i, ProjectErrorUtils.serializeEvaluationException(exception));
            }
          }
        }
        return;

      case NONE:
        if (!trackRecordLevelErrors) {
          for (int i = 0; i < recordCount; i++) {
            doEval(i, i);
          }
        } else {
          for (int i = 0; i < recordCount; i++) {
            try {
              doEval(i, i);
            } catch (Exception exception) {
              errorVector.setSafe(i, ProjectErrorUtils.serializeEvaluationException(exception));
            }
          }
        }
        for (TransferPair t : transfers) {
          t.transfer();
        }
        return;

      case FOUR_BYTE:
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public final void setup(
      FunctionContext context,
      VectorAccessible incoming,
      VectorAccessible outgoing,
      List<TransferPair> transfers,
      ComplexWriterCreator writerCreator,
      ExpressionEvaluationOptions projectorOptions)
      throws SchemaChangeException {

    this.svMode = incoming.getSchema().getSelectionVectorMode();
    switch (svMode) {
      case NONE:
        break;
      case TWO_BYTE:
        this.vector2 = incoming.getSelectionVector2();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported selection vector mode " + svMode.name());
    }
    if (outgoing != null
        && projectorOptions != null
        && projectorOptions.isTrackRecordLevelErrors()) {
      TypedFieldId errorFieldId =
          outgoing.getValueVectorId(BasePath.getSimple(ColumnUtils.COPY_HISTORY_COLUMN_NAME));
      if (errorFieldId != null) {
        errorVector =
            VectorResolver.simple(outgoing, VarCharVector.class, errorFieldId.getFieldIds());
        trackRecordLevelErrors = true;
      }
    }
    this.transfers = ImmutableList.copyOf(transfers);
    doSetup(context, incoming, outgoing, writerCreator);
  }

  public abstract void doSetup(
      @Named("context") FunctionContext context,
      @Named("incoming") VectorAccessible incoming,
      @Named("outgoing") VectorAccessible outgoing,
      @Named("writerCreator") ComplexWriterCreator writerCreator);

  public abstract void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
}

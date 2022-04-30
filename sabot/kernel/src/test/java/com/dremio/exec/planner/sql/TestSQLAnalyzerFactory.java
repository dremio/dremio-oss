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
package com.dremio.exec.planner.sql;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.junit.Test;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionList;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.users.SystemUser;

/**
 * Tests {@link TestSQLAnalyzerFactory}
 */
public class TestSQLAnalyzerFactory {

  @Test
  public void testCreationOfValidator() {
    SabotContext sabotContext = mock(SabotContext.class);
    FunctionImplementationRegistry functionImplementationRegistry = mock(FunctionImplementationRegistry.class);
    CatalogService catalogService = mock(CatalogService.class);
    Catalog catalog = mock(Catalog.class);
    ProjectOptionManager mockOptions = mock(ProjectOptionManager.class);
    when(mockOptions.getOptionValidatorListing()).thenReturn(mock(OptionValidatorListing.class));

    // Stub appropriate methods.
    when(sabotContext.getFunctionImplementationRegistry()).thenReturn(functionImplementationRegistry);
    when(sabotContext.getCatalogService()).thenReturn(catalogService);
    when(sabotContext.getCatalogService().getCatalog(any(MetadataRequestOptions.class))).thenReturn(catalog);

    OptionValue value1 = OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, PlannerSettings.ENABLE_DECIMAL_V2_KEY, false);
    OptionValue value2 = OptionValue.createLong(OptionValue.OptionType.SYSTEM, UserSession.MAX_METADATA_COUNT.getOptionName(), 0);
    OptionList optionList = new OptionList();
    optionList.add(value1);
    optionList.add(value2);

    when(mockOptions.getOption(PlannerSettings.ENABLE_DECIMAL_V2_KEY)).thenReturn(value1);
    when(mockOptions.getOption(UserSession.MAX_METADATA_COUNT.getOptionName())).thenReturn(value2);
    when(mockOptions.getNonDefaultOptions()).thenReturn(optionList);

    // Test that the correct concrete implementation is created.
    SQLAnalyzer sqlAnalyzer = SQLAnalyzerFactory.createSQLAnalyzer(SystemUser.SYSTEM_USERNAME, sabotContext, null, true, mockOptions);
    SqlValidatorWithHints validator = sqlAnalyzer.validator;
    assertTrue(validator instanceof SqlAdvisorValidator);

    sqlAnalyzer = SQLAnalyzerFactory.createSQLAnalyzer(SystemUser.SYSTEM_USERNAME, sabotContext, null, false, mockOptions);
    validator = sqlAnalyzer.validator;
    assertTrue(validator instanceof SqlValidatorImpl);
  }
}

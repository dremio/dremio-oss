/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic;

import com.dremio.plugins.elastic.ElasticBaseTestQuery.ScriptsEnabled;

/**
 * Test for elasticsearch contains with scripts disabled.  Runs all the tests in parent class with scripts disabled.
 */
@ScriptsEnabled(enabled = false)
public class TestElasticsearchContainsWithScriptsDisabled extends TestElasticsearchContains {
}

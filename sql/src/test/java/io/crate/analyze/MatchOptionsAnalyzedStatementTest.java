/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MatchOptionsAnalyzedStatementTest extends CrateUnitTest {

    @Test
    public void testValidOptions() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("analyzer", new BytesRef("english"));
        options.put("operator", new BytesRef("and"));
        options.put("fuzziness", 12);
        // no exception raised
        MatchOptionsAnalysis.validate(options);
    }

    @Test
    public void testUnknownMatchOptions() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("unknown match option 'analyzer_wrong'");

        Map<String, Object> options = new HashMap<>();
        options.put("analyzer_wrong", new BytesRef("english"));
        MatchOptionsAnalysis.validate(options);
    }

    @Test
    public void testInvalidMatchValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid value for option 'max_expansions': abc");

        Map<String, Object> options = new HashMap<>();
        options.put("max_expansions", new BytesRef("abc"));
        MatchOptionsAnalysis.validate(options);
    }

    @Test
    public void testZeroTermsQueryMustBeAString() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid value for option 'zero_terms_query': 12.6");
        MatchOptionsAnalysis.validate(ImmutableMap.<String, Object>of("zero_terms_query", 12.6));
    }

    @Test
    public void testUnknownOption() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("unknown match option 'oh'");
        MatchOptionsAnalysis.validate(ImmutableMap.<String, Object>of("oh", 1));
    }
}

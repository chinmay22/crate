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
package io.crate.operation.operator;

import io.crate.analyze.symbol.Literal;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.operation.operator.LikeOperator.DEFAULT_ESCAPE;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class LikeOperatorTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeSymbolEqual() {
        assertNormalize("'foo' like 'foo'", isLiteral(true));
        assertNormalize("'notFoo' like 'foo'", isLiteral(false));
    }

    @Test
    public void testPatternIsNoLiteral() throws Exception {
        assertEvaluate("name like timezone", false, Literal.of("foo"), Literal.of("bar"));
        assertEvaluate("name like name", true, Literal.of("foo"), Literal.of("foo"));
    }

    @Test
    public void testNormalizeSymbolLikeZeroOrMore() {
        // Following tests: wildcard: '%' ... zero or more characters (0...N)

        assertNormalize("'foobar' like '%bar'", isLiteral(true));
        assertNormalize("'bar' like '%bar'", isLiteral(true));
        assertNormalize("'ar' like '%bar'", isLiteral(false));
        assertNormalize("'foobar' like 'foo%'", isLiteral(true));
        assertNormalize("'foo' like 'foo%'", isLiteral(true));
        assertNormalize("'fo' like 'foo%'", isLiteral(false));
        assertNormalize("'foobar' like '%oob%'", isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolLikeExactlyOne() {
        // Following tests: wildcard: '_' ... any single character (exactly one)
        assertNormalize("'bar' like '_ar'", isLiteral(true));
        assertNormalize("'bar' like '_bar'", isLiteral(false));
        assertNormalize("'foo' like 'fo_'", isLiteral(true));
        assertNormalize("'foo' like 'foo_'", isLiteral(false));
        assertNormalize("'foo' like '_o_'", isLiteral(true));
        assertNormalize("'foobar' like '_foobar_'", isLiteral(false));
    }

    // Following tests: mixed wildcards:

    @Test
    public void testNormalizeSymbolLikeMixed() {
        assertNormalize("'foobar' like '%o_ar'", isLiteral(true));
        assertNormalize("'foobar' like '%a_'", isLiteral(true));
        assertNormalize("'foobar' like '%o_a%'", isLiteral(true));

        assertNormalize("'Lorem ipsum dolor...' like '%i%m%'", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like '%%%sum%%'", isLiteral(true));
        assertNormalize("'Lorem ipsum dolor...' like '%i%m'", isLiteral(false));
    }

    // Following tests: escaping wildcards

    @Test
    public void testExpressionToRegexExactlyOne() {
        String expression = "fo_bar";
        assertEquals("^fo.bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testLikeOnMultilineStatement() throws Exception {
        String stmt = "SELECT date_trunc('day', ts), sum(num_steps) as num_steps, count(*) as num_records \n" +
                      "FROM steps\n" +
                      "WHERE month_partition = '201409'\n" +
                      "GROUP BY 1 ORDER BY 1 DESC limit 100";

        assertEvaluate("name like '  SELECT%'", false, Literal.of(stmt));
        assertEvaluate("name like 'SELECT%'", true, Literal.of(stmt));
        assertEvaluate("name like 'SELECT date_trunc%'", true, Literal.of(stmt));
        assertEvaluate("name like '% date_trunc%'", true, Literal.of(stmt));
    }

    @Test
    public void testExpressionToRegexZeroOrMore() {
        String expression = "fo%bar";
        assertEquals("^fo.*bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscapingPercent() {
        String expression = "fo\\%bar";
        assertEquals("^fo%bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscapingUnderline() {
        String expression = "fo\\_bar";
        assertEquals("^fo_bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscaping() {
        String expression = "fo\\\\_bar";
        assertEquals("^fo\\\\.bar$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexEscapingMutli() {
        String expression = "%%\\%sum%%";
        assertEquals("^.*.*%sum.*.*$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testExpressionToRegexMaliciousPatterns() {
        String expression = "fo(ooo)o[asdf]o\\bar^$.*";
        assertEquals("^fo\\(ooo\\)o\\[asdf\\]obar\\^\\$\\.\\*$", LikeOperator.patternToRegex(expression, DEFAULT_ESCAPE, true));
    }

    @Test
    public void testLikeOperator() {
        assertEvaluate("'foobarbaz' like 'foo%baz'", true);
        assertEvaluate("'foobarbaz' like 'foo_baz'", false);
        assertEvaluate("'characters' like 'charac%'", true);

        assertEvaluate("'foobarbaz' like name", null, Literal.NULL);
        assertEvaluate("name like 'foobarbaz'", null, Literal.NULL);
    }
}

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
import io.crate.operation.operator.any.AnyOperator;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class InOperatorTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeSymbolSetLiteralIntegerIncluded() {
        assertNormalize("1 in (1, 2, 4, 8)", isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolSetLiteralIntegerNotIncluded() {
        assertNormalize("128 in (1, 2, 4, 8)", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolSetLiteralDifferentDataTypeValue() {
        assertNormalize("2.3 in (1, 2, 4, 8)", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolSetLiteralReference() {
        assertNormalize("age in (1, 2)", isFunction(AnyOperator.OPERATOR_PREFIX + "="));
    }

    @Test
    public void testNormalizeSymbolSetLiteralStringIncluded() {
        assertNormalize("'charlie' in ('alpha', 'bravo', 'charlie', 'delta')", isLiteral(true));
    }

    @Test
    public void testNormalizeSymbolSetLiteralStringNotIncluded() {
        assertNormalize("'not included' in ('alpha', 'bravo', 'charlie', 'delta')", isLiteral(false));
    }

    @Test
    public void testEvaluateInOperator() {
        assertEvaluate("null in ('alpha', 'bravo')", null);
        assertEvaluate("name in ('alpha', 'bravo')", null, Literal.of(DataTypes.STRING, null));
        assertEvaluate("null in (name)", null, Literal.of(DataTypes.STRING, null));
        assertEvaluate("'alpha' in ('alpha', null)", true);
        assertEvaluate("'alpha' in (null, 'alpha')", true);
        assertEvaluate("'alpha' in ('beta', null)", null);
        assertEvaluate("'alpha' in (null)", null);
        assertEvaluate("null in (null)", null);
    }
}

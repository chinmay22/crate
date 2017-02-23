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

package io.crate.operation.scalar.arithmetic;

import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;


public class AbsFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testAbs() throws Exception {
        assertEvaluate("abs(-2)", 2L);
        assertEvaluate("abs(-2.0)", 2.0);
        assertEvaluate("abs(cast(-2 as integer))", 2);
        assertEvaluate("abs(cast(-2.0 as float))", 2.0f);
        assertEvaluate("abs(null)", null);
    }

    @Test
    public void testWrongType() throws Exception {
        expectedException.expectMessage("unknown function: abs(string)");
        assertEvaluate("abs('foo')", null);
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("abs(id)", isFunction("abs"));
    }

    @Test
    public void testNormalizeNull() throws Exception {
        assertNormalize("abs(null)", isLiteral(null));
    }
}

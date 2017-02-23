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


public class SquareRootFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testSqrt() throws Exception {
        assertEvaluate("sqrt(25)", 5.0d);
        assertEvaluate("sqrt(25.0)", 5.0d);
        assertEvaluate("sqrt(null)", null);
        assertEvaluate("sqrt(cast(25 as integer))", 5.0);
        assertEvaluate("sqrt(cast(25.0 as float))", 5.0);
    }

    @Test
    public void testSmallerThanZero() throws Exception {
        expectedException.expectMessage("cannot take square root of a negative number");
        assertEvaluate("sqrt(-25.0)", null);
    }

    @Test
    public void testInvalidType() throws Exception {
        expectedException.expectMessage("unknown function: sqrt(string)");
        assertEvaluate("sqrt('foo')", null);
    }

    @Test
    public void testNormalizeWithRef() throws Exception {
        assertNormalize("sqrt(id)", isFunction("sqrt"));
    }
}

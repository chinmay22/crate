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
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class CeilFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluateOnDouble() throws Exception {
        assertNormalize("ceil(29.9)", isLiteral(30L));
        assertNormalize("ceil(null)", isLiteral(null));
    }

    @Test
    public void testEvaluateOnFloat() throws Exception {
        assertNormalize("ceil(cast(29.9 as float))", isLiteral(30));
        assertNormalize("ceil(cast(null as float))", isLiteral(null, DataTypes.INTEGER));
    }

    @Test
    public void testEvaluateOnIntAndLong() throws Exception {
        assertNormalize("ceil(cast(20 as integer))", isLiteral(20));
        assertNormalize("ceil(cast(null as integer))", isLiteral(null, DataTypes.INTEGER));

        assertNormalize("ceil(20)", isLiteral(20L));
        assertNormalize("ceil(cast(null as long))", isLiteral(null, DataTypes.LONG));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("ceil(age)", isFunction(CeilFunction.NAME));
    }
}

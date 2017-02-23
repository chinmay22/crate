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

package io.crate.core.collections;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.TreeMap;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TreeMapBuilderTest extends CrateUnitTest {

    @Test
    public void testBuilder() throws Exception {
        TreeMapBuilder<Integer, Integer> builder = TreeMapBuilder.newMapBuilder();

        assertThat(builder.put(1, 1), instanceOf(TreeMapBuilder.class));
        assertTrue(builder.containsKey(1));
        assertThat(builder.get(1), is(1));
        assertFalse(builder.isEmpty());

        assertThat(builder.remove(1), instanceOf(TreeMapBuilder.class));
        assertTrue(builder.isEmpty());

        builder.put(1, 1);
        assertThat(builder.map(), instanceOf(TreeMap.class));

        TreeMapBuilder<Integer, Integer> builder2 = TreeMapBuilder.<Integer, Integer>newMapBuilder();
        assertThat(builder2.putAll(builder.map()), instanceOf(TreeMapBuilder.class));
        assertThat(builder2.get(1), is(1));
        assertFalse(builder2.isEmpty());

        assertThat(builder2.clear(), instanceOf(TreeMapBuilder.class));
        assertTrue(builder2.isEmpty());
    }
}

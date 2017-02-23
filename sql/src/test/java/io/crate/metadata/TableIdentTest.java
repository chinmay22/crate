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

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class TableIdentTest extends CrateUnitTest {

    @Test
    public void testIndexName() throws Exception {
        TableIdent ti = new TableIdent(null, "t");
        assertThat(ti.indexName(), is("t"));
        ti = new TableIdent("s", "t");
        assertThat(ti.indexName(), is("s.t"));
    }

    @Test
    public void testFromIndexName() throws Exception {
        assertThat(TableIdent.fromIndexName("t"), is(new TableIdent(null, "t")));
        assertThat(TableIdent.fromIndexName("s.t"), is(new TableIdent("s", "t")));

        PartitionName pn = new PartitionName("s", "t", ImmutableList.of(new BytesRef("v1")));
        assertThat(TableIdent.fromIndexName(pn.asIndexName()), is(new TableIdent("s", "t")));

        pn = new PartitionName(null, "t", ImmutableList.of(new BytesRef("v1")));
        assertThat(TableIdent.fromIndexName(pn.asIndexName()), is(new TableIdent(null, "t")));
    }

    @Test
    public void testDefaultSchema() throws Exception {
        TableIdent ti = new TableIdent(null, "t");
        assertThat(ti.schema(), is("doc"));
        assertThat(ti, is(new TableIdent("doc", "t")));
    }

    @Test
    public void testFQN() throws Exception {
        TableIdent ti = new TableIdent(null, "t");
        assertThat(ti.fqn(), is("doc.t"));

        ti = new TableIdent("s", "t");
        assertThat(ti.fqn(), is("s.t"));
    }
}

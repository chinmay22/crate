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

package io.crate.integrationtests;


import io.crate.common.Hex;
import io.crate.test.utils.Blobs;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Locale;

import static org.hamcrest.Matchers.is;

public abstract class SQLHttpIntegrationTest extends SQLTransportIntegrationTest {

    private HttpPost httpPost;
    private InetSocketAddress address;
    private CloseableHttpClient httpClient = HttpClients.createDefault();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("http.enabled", true)
            .put("http.host", "127.0.0.1")
            .build();
    }

    @Before
    public void setup() {
        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        address = ((InetSocketTransportAddress) httpServerTransport.boundAddress().publishAddress())
            .address();
        httpPost = new HttpPost(String.format(Locale.ENGLISH, "http://%s:%s/_sql?error_trace", address.getHostName(), address.getPort()));
    }


    protected CloseableHttpResponse post(String body, @Nullable Header[] headers) throws IOException {
        if (body != null) {
            StringEntity bodyEntity = new StringEntity(body, ContentType.APPLICATION_JSON);
            httpPost.setEntity(bodyEntity);
        }
        httpPost.setHeaders(headers);
        return httpClient.execute(httpPost);
    }

    protected CloseableHttpResponse post(String body) throws IOException {
        return post(body, null);
    }

    protected String upload(String table, String content) throws IOException {
        String digest = blobDigest(content);
        String url = Blobs.url(address, table, digest);
        HttpPut httpPut = new HttpPut(url);
        httpPut.setEntity(new StringEntity(content));

        CloseableHttpResponse response = httpClient.execute(httpPut);
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        response.close();

        return url;
    }

    protected String blobDigest(String content) {
        return Hex.encodeHexString(Blobs.digest(content));
    }
}

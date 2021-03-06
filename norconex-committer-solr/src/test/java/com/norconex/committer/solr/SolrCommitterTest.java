/* Copyright 2010-2014 Norconex Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.norconex.committer.solr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.norconex.committer.solr.SolrCommitter.ISolrServerFactory;
import com.norconex.commons.lang.config.ConfigurationUtil;
import com.norconex.commons.lang.map.Properties;

/**
 * To run these tests under Eclipse, you have to enable JVM assertions (-ea).
 * Under Maven, the surefire plugin enables them by default.
 * 
 * @author Pascal Dimassimo
 */
public class SolrCommitterTest extends AbstractSolrTestCase {

    //TODO test update/delete URL params
    
    static {
        System.setProperty("solr.allow.unsafe.resourceloading", "true");
        ClassLoader loader = SolrCommitterTest.class.getClassLoader();
        loader.setPackageAssertionStatus("org.apache.solr", true);
        loader.setPackageAssertionStatus("org.apache.lucene", true);
    }
    
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private EmbeddedSolrServer server;

    private SolrCommitter committer;

    private File queue;

    @Before
    public void setup() throws Exception {

        File solrHome = tempFolder.newFolder("solr");
        initCore("src/test/resources/solrconfig.xml",
                "src/test/resources/schema.xml", solrHome.toString());

        server = new EmbeddedSolrServer(h.getCoreContainer(), h.getCore()
                .getName());
        
        committer = new SolrCommitter(new ISolrServerFactory() {
            private static final long serialVersionUID = 4648990433469043210L;
            @Override
            public SolrServer createSolrServer(SolrCommitter solrCommitter) {
                return server;
            }
        });

        queue = tempFolder.newFolder("queue");
        committer.setQueueDir(queue.toString());
    }

    @After
    public void teardown() {
        deleteCore();
    }

    @Test
    public void testCommitAdd() throws Exception {

        String content = "hello world!";
        InputStream is = IOUtils.toInputStream(content);
        
        String id = "1";
        Properties metadata = new Properties();
        metadata.addString("id", id);

        
        // Add new doc to Solr
        committer.add(id, is, metadata);

        committer.commit();

        IOUtils.closeQuietly(is);
        
        // Check that it's in Solr
        SolrDocumentList results = queryId(id);
        assertEquals(1, results.getNumFound());
        assertEquals(id,
                results.get(0).get(SolrCommitter.DEFAULT_SOLR_ID_FIELD));
        // TODO we need to trim because of the extra white spaces returned by
        // Solr. Why is that?
        assertEquals(content, results.get(0).get(
                SolrCommitter.DEFAULT_SOLR_CONTENT_FIELD).toString().trim());
    }

    @Test
    public void testCommitDelete() throws Exception {

        // Add a document directly to Solr
        SolrInputDocument doc = new SolrInputDocument();
        String id = "1";
        doc.addField(SolrCommitter.DEFAULT_SOLR_ID_FIELD, id);
        String content = "hello world!";
        doc.addField(SolrCommitter.DEFAULT_SOLR_CONTENT_FIELD, content);
        server.add(doc);
        server.commit();

        // Queue it to be deleted
        Properties metadata = new Properties();
        metadata.addString("id", id);
        committer.remove(id, metadata);

        committer.commit();

        // Check that it's remove from Solr
        SolrDocumentList results = queryId(id);
        assertEquals(0, results.getNumFound());
    }

    private SolrDocumentList queryId(String id) throws SolrServerException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", String.format("%s:%s",
                SolrCommitter.DEFAULT_SOLR_ID_FIELD, id));
        QueryResponse response = server.query(solrParams);
        SolrDocumentList results = response.getResults();
        return results;
    }
    
    @Test
    public void testWriteRead() throws IOException {
        SolrCommitter outCommitter = new SolrCommitter();
        outCommitter.setQueueDir("C:\\FakeTestDirectory\\");
        outCommitter.setSourceContentField("sourceContentField");
        outCommitter.setTargetContentField("targetContentField");
        outCommitter.setSourceReferenceField("idTargetField");
        outCommitter.setTargetReferenceField("idTargetField");
        outCommitter.setKeepSourceContentField(true);
        outCommitter.setKeepSourceReferenceField(false);
        outCommitter.setQueueSize(100);
        outCommitter.setCommitBatchSize(50);
        outCommitter.setSolrURL("http://solrurl.com/test");
        outCommitter.setUpdateUrlParam("uparam1", "uvalue1");
        outCommitter.setUpdateUrlParam("uparam2", "uvalue2");
//        outCommitter.setDeleteUrlParam("dparam1", "dvalue1");
//        outCommitter.setDeleteUrlParam("dparam2", "dvalue2");
        System.out.println("Writing/Reading this: " + outCommitter);
        ConfigurationUtil.assertWriteRead(outCommitter);
    }
    
}

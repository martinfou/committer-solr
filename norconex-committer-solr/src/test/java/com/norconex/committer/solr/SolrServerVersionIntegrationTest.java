package com.norconex.committer.solr;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
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
import com.norconex.commons.lang.map.Properties;

public class SolrServerVersionIntegrationTest extends AbstractSolrTestCase{

    //TODO test update/delete URL params
    
    static {
        System.setProperty("solr.allow.unsafe.resourceloading", "true");
        ClassLoader loader = SolrCommitterTest.class.getClassLoader();
        loader.setPackageAssertionStatus("org.apache.solr", true);
        loader.setPackageAssertionStatus("org.apache.lucene", true);
    }
    
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private SolrClient server;
    private String solrUrl = "http://localhost:8983/solr";
    private SolrCommitter committer;
    private File queue;

    @Before
    public void setup() throws Exception  {
        File solrHome = tempFolder.newFolder("solr");
            initCore("src/test/resources/solrconfig.xml",
                    "src/test/resources/schema.xml", solrHome.toString());

        server = new HttpSolrClient(solrUrl);
        
        committer = new SolrCommitter(new ISolrServerFactory() {
            private static final long serialVersionUID = 4648990433469043210L;
            @Override
            public SolrClient createSolrServer(SolrCommitter solrCommitter) {
                return server;
            }
        });
        committer.setUpdateUrlParam("commitWithin", "1");

        queue = tempFolder.newFolder("queue");
        committer.setQueueDir(queue.toString());
    }
    

    @After
    public void teardown() throws SolrServerException, IOException {
     }
    
    
    @Test
    public void add1DocumentToSolr3x() throws SolrServerException, IOException {

        String content = "hello world!";
        InputStream is = IOUtils.toInputStream(content);
        
        String id = "1";
        Properties metadata = new Properties();
        metadata.addString("id", id);
        
        // Add new doc to Solr
        committer.add(id, is, metadata);
        committer.commit();

        IOUtils.closeQuietly(is);
        assertEquals(1, getAllDocsCount());
        cleanDb();
    }
    
    private long getAllDocsCount() throws SolrServerException{
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
          solrParams.set("q", "*:*");
        QueryResponse response = server.query(solrParams);
        long results = response.getResults().getNumFound();
        return results;
    }
    
    private void cleanDb() throws SolrServerException, IOException{
        UpdateResponse response = server.deleteByQuery("*:*");
        server.commit();
        System.out.println("response = "+response);
    }

}

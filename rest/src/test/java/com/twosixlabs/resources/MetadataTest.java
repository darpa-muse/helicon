package com.twosixlabs.resources;

import com.twosixlabs.model.entities.CorpusMetadata;
import com.twosixlabs.model.entities.UniqueCorpusMetadata;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class MetadataTest extends ResourceTest {
/*
    @BeforeClass
    public static void init() {
        // Override zookeeper port to port mapped
        ScanWrapper.ZOOKEEPER_PORT = "44001";
    }

    @Override
    protected Application configure() {
        return App.resourceConfig;
    }
*/

    @Test
    public void testServeCorpusMetadataKeys() {
        String path = "muse/metadata/keys/pagesize/1/page/1";
        testMsg(path);
        Long count = target(path)
                .request().get(UniqueCorpusMetadata.class).getCount();

        assertEquals((Long)1L, count);
    }

    @Test
    public void testServeProjectMetadataValues() {
        //"metadata/{key}/pagesize/{pageSize}/page/{page}"
        String path = "muse/metadata/project_size/pagesize/1/page/1";
        testMsg(path);

        int count = target(path)
                .request().get(CorpusMetadata.class).getCount();

        assertEquals(1, count);
    }

    @Test
    public void testServeUniqueProjectMetadataValues() {
        // "metadata/{key}/pagesize/{pageSize}/page/{page}/unique"
        String path = "muse/metadata/project_size/pagesize/1/page/1/unique";
        testMsg(path);
        Long count = target(path)
                .request().get(UniqueCorpusMetadata.class).getCount();

        assertEquals((Long)1L, count);
    }

}

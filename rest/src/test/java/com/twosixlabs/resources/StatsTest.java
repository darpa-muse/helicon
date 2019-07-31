package com.twosixlabs.resources;

import com.twosixlabs.model.entities.CorpusMetadata;
import com.twosixlabs.model.entities.UniqueCorpusMetadata;
import org.junit.Test;

import javax.ws.rs.core.MediaType;

import static junit.framework.TestCase.assertEquals;

public class StatsTest extends ResourceTest{

    @Test
    public void testServeAveMetadataValues() {
        String path = "";
        testMsg(path);

        Long count = target("muse/stats/ave/metadata/project_size/pagesize/1/page/1")
                .request().accept(MediaType.APPLICATION_JSON_TYPE).get(UniqueCorpusMetadata.class).getCount();

        assertEquals((Long)1L, count);
    }

    @Test
    public void testServeCountMetadataValues() {
        // count/metadata/{key}/pagesize/{pageSize}/page/{page}
        String path = "muse/stats/count/metadata/project_size/pagesize/1/page/1";
        testMsg(path);

        Long count = target(path)
                .request().get(UniqueCorpusMetadata.class).getCount();

        assertEquals((Long)1L, count);  // the count hold one number that is the number of project_size
     }

     @Test
     public void testServeMinMaxMetadataValues() {
        // minmax/metadata/{key}/pagesize/{pageSize}/page/{page}
         String path = "muse/stats/minmax/metadata/project_size/pagesize/2/page/1";
         testMsg(path);

         int count = target(path)
                 .request().get(CorpusMetadata.class).getCount();

         assertEquals(2, count); // there's a min and max value
    }
}

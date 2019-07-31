package com.twosixlabs.resources;

import com.twosixlabs.model.entities.CorpusMetadata;
import com.twosixlabs.model.entities.ProjectMetadata;
import org.apache.accumulo.core.util.Base64;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.util.HashSet;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;

public class CategoryItemTest extends ResourceTest{
    String languageBase64 = Base64.encodeBase64String("java".getBytes());
    String projectRow = Base64.encodeBase64String("5.0|java 2 xml binding|2012-06-14|uci2011|33e3df88-cdd6-11e4-9ba4-e7d025d4bed0".getBytes());
    String languagesPathRoot ="muse/categories/languages";
    String topicsPathRootBase64 = Base64.encodeBase64String("muse/categories/topics".getBytes());

    @Test
    public void testGetCategoryJson() {
        testMsg(languagesPathRoot);
        int count = target(languagesPathRoot)
                .request().accept(MediaType.APPLICATION_JSON_TYPE).get(CorpusMetadata.class).getCount();

        assertTrue(count > 1); // there should be at least 1 language
    }

    @Test
    // @Path("{category}/{categoryItem: .+|.+\\/pages}")
    public void testServeCategoryBasedProjectsJson(){
        String path = languagesPathRoot +"/"+languageBase64;
        testMsg(path);

        int count = target(path).request()
                .accept(MediaType.APPLICATION_JSON_TYPE).get(HashSet.class).size();

        assertTrue(count > 1); // there should be at least 1 language
    }

    @Test
    //@Path("{category}/{categoryItem}/pages/{page: \\d+|\\d+\\/projects}")
    //@Produces(MediaType.APPLICATION_JSON)
    public void testServeCategoryBasedProjectsPerPageJson() {
        String path = languagesPathRoot +"/"+languageBase64+"/pages/1";
        testMsg(path);
        int count = target(path)
                .request().accept(MediaType.APPLICATION_JSON_TYPE).get(HashSet.class).size();

        assertTrue(count > 1); // there should be at least 1 language
    }

    //   @Path("{category}/{categoryItem}/pages/{page}/projects/{projectRow}")
    //   @Produces(MediaType.APPLICATION_JSON)
    @Test
    public void testServeCategoryBasedProjectJson() {
        String path = languagesPathRoot +"/"+languageBase64+"/pages/1/projects/"+projectRow;
        testMsg(path);
        int count = target(path).request()
                .accept(MediaType.APPLICATION_JSON_TYPE).get(ProjectMetadata.class).getMetadata().size();

        assertTrue(count > 1); // there should be at least 1 feature for this project
    }

    //    @Path("{category}/{categoryItem}/pages/{page}/projects/{projectRow}/metadata/{key}")
//    @Produces(MediaType.APPLICATION_JSON)
    @Test
    public void testServeCategoryBasedProjectMetadataJson() {
        String key = Base64.encodeBase64String("project_size".getBytes());
        String path = languagesPathRoot +"/" + languageBase64 + "/pages/1/projects/"+projectRow+"/metadata/"+key;
        testMsg(path);
        int count = target(path).request().accept(MediaType.APPLICATION_JSON_TYPE).get(ProjectMetadata.class).getMetadata().size();

        assertTrue(count == 1); // there should be at least 1 feature for this project
    }

    //    @Path("{category}/{categoryItem}/pages/{page}/projects/{projectRow}/content/{type: code|metadata}")
    @Test
    public void testServeCategoryBasedProjectContentCodeJson() {
        String key = Base64.encodeBase64String("project_size".getBytes());
        String path = languagesPathRoot +"/"+ languageBase64 +"/pages/1/projects/"+projectRow+"/content/code";
        testMsg(path);
        int count = target(path).request().accept(MediaType.APPLICATION_JSON_TYPE).get(ProjectMetadata.class).getMetadata().size();

        assertTrue(count == 1); // there should be at least 1 feature for this project

    }
    @Test
    public void testServeCategoryBasedProjectContentMetadataJson() {
        String path = languagesPathRoot +"/"+ languageBase64 +"/pages/1/projects/"+projectRow+"/content/metadata";
        int count = target(path).request().accept(MediaType.APPLICATION_JSON_TYPE).get(ProjectMetadata.class).getMetadata().size();

        assertTrue(count == 1); // there should be at least 1 feature for this project
    }

}

package com.twosixlabs.model;

import com.google.gson.Gson;
import com.twosixlabs.model.entities.*;
import com.twosixlabs.resources.ResourceTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class EntityTest extends ResourceTest {
    /*
    ### ProjectMetadata
    ### CorpusMetadata
    ### UniqueCorpusMetadata
    ### MetadataValueStats
    ### MuseResponse
     */

    Gson gson = new Gson();

    @Test
    public void testMetadataValueStats() {
        testMsg("-- testMetadataValueStats --");

        MetadataValueStats metadataValueStats = new MetadataValueStats();
        metadataValueStats.setCount(0);
        metadataValueStats.setMax(0.0);
        metadataValueStats.setMean(0);
        metadataValueStats.setMin(0);
        metadataValueStats.setMode(0);
        metadataValueStats.setStd(0);
        metadataValueStats.setSum(0);
        metadataValueStats.setVariance(0);
        metadataValueStats.setKey("");
        metadataValueStats.setUrl("");

        testMsg(gson.toJson(metadataValueStats));
    }
    @Test
    public void testMuseResponse() {
        testMsg("-- testMetadataValueStats --");
        MuseResponse museResponse = new MuseResponse();
        museResponse.setMessage("This holds the message set by the endpoint");
        museResponse.setKey("Since this is used in multiple contents, it holds the metadata key");
        museResponse.setUrl("This is the called URL");
    //    museResponse.setElapsedTimeMsec("Time elapsed to complete the call.");
        museResponse.setValue("Holds the value; typically a metadata value");
        testMsg(gson.toJson(museResponse));
    }
    @Test
    public void testUniqueCorpusMetadata() {
        testMsg("-- testMetadataValueStats --");
        UniqueCorpusMetadata uniqueCorpusMetadata = new UniqueCorpusMetadata();
        uniqueCorpusMetadata.setValues(new HashSet<>());
        uniqueCorpusMetadata.setValue("Holds a generic value.");
        uniqueCorpusMetadata.setCount(0l);
        uniqueCorpusMetadata.setKey("Holds a generic key");
        uniqueCorpusMetadata.setUrl("Holds the called URL");
        uniqueCorpusMetadata.setElapsedTimeMsec(0);

        testMsg(gson.toJson(uniqueCorpusMetadata));
    }
    @Test
    public void testCorpusMetadata() {
        testMsg("-- testMetadataValueStats --");

        CorpusMetadata corpusMetadata = new CorpusMetadata();
        corpusMetadata.setMapValues(new ConcurrentHashMap<>());
        corpusMetadata.setArrayValues(new ArrayList<>());
        corpusMetadata.setCount(0);
        corpusMetadata.setKey("Generic Key");
        corpusMetadata.setUrl("Called URL");
        corpusMetadata.setValue("Generic Value");
        corpusMetadata.setElapsedTimeMsec(0);

        testMsg(gson.toJson(corpusMetadata));
    }

    @Test
    public void testProjectMetadata() {
        testMsg("-- testMetadataValueStats --");
        ProjectMetadata projectMetadata = new ProjectMetadata();
        projectMetadata.setRowId("RowId is the main key in the key/value store.");
        projectMetadata.setCodeUrl("This is the URL for where the code of the project is located");
        projectMetadata.setMetadata(new HashMap<>());
        projectMetadata.setKey("Generic Key");
        projectMetadata.setElapsedTimeMsec(0);
        projectMetadata.setMetadataUrl("This is the URL for where the metadata file for the project is located");
        projectMetadata.setMiniMetadataUrl("This is the URL for where the truncated metadata file is located");
        projectMetadata.setValue("Holds a generic value");

        testMsg(gson.toJson(projectMetadata));
    }
}

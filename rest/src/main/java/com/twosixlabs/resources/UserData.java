package com.twosixlabs.resources;

import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.MuseResponse;
import com.twosixlabs.model.entities.ProjectMetadata;
import com.twosixlabs.model.entities.UserDataRecord;
import com.twosixlabs.muse_utils.App;
import com.twosixlabs.muse_utils.Security;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.twosixlabs.model.accumulo.ScanWrapper.ALL_METADATA;
import static com.twosixlabs.model.accumulo.ScanWrapper.PROJECT_FILES_METADATA;
import static com.twosixlabs.model.accumulo.ScanWrapper.PROJECT_FILES_METADATA_CF;


@Path("muse/projects")
public class UserData extends ResourceBase {

    ////////////////// projectMetadata /////////////////
    @POST
    @Path("{projectRowBase64}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse postToProjectMetadata(@PathParam("projectRowBase64") String projectRowBase64,
                                              UserDataRecord userDataRecord){
        MuseResponse rVal = null;
        try {
            Security.validateAndCompleteUserDataRecord(getApiKey(),userDataRecord);
            long start = System.currentTimeMillis();
            String projectRow = decode64(projectRowBase64);
            ScanWrapper.postUserDataRec(getApiKey(), projectRow,
                    ScanWrapper.PROJECT_METADATA_CF, userDataRecord);

            rVal = new MuseResponse();
            rVal.setUrl(getAbsoluteUrl());
            rVal.setKey(userDataRecord.getKey());
            String s = userDataRecord.getValue();
            rVal.setValue(new StringBuilder("(1st 100 Characters)- ").append(s.substring(0, Math.min(s.length(), 100))).toString());
            rVal.setMessage(new StringBuilder("Project metadata data written to: ").append(projectRow).toString());
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), e.getMessage()).build());
        }
        return rVal;
    }

    @POST
    @Path("{projectRowBase64}/files/{fileHash}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse postToContentMetadata(@PathParam("projectRowBase64") String projectRowBase64,
                                              @PathParam("fileHash") String fileHash,
                                              UserDataRecord userDataRecord){
        MuseResponse rVal = null;
        try {
            Security.validateAndCompleteUserDataRecord(getApiKey(),userDataRecord);
            long start = System.currentTimeMillis();
            String projectRow = decode64(projectRowBase64);

            // for file based metadata the key pattern is:
            // <metadata>|<file hash>
            userDataRecord.setKey(new StringBuilder(userDataRecord.getKey())
                    .append("::").append(fileHash).toString());

            ScanWrapper.postUserDataRec(getApiKey(), projectRow,
                    ScanWrapper.PROJECT_FILES_METADATA_CF, userDataRecord);

            rVal = new MuseResponse();
            rVal.setUrl(getAbsoluteUrl());
            rVal.setKey(userDataRecord.getKey());
            String s = userDataRecord.getValue();
            rVal.setValue(new StringBuilder("(1st 100 Characters)- ").append(s.substring(0, Math.min(s.length(), 100))).toString());
            rVal.setMessage(new StringBuilder("Content metadata written to: ").append(projectRow).toString());
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), e.getMessage()).build());
        }
        return rVal;
    }

    // obtains a project where the category
    // is not known or used
    @GET
    @Path("{projectRowBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectMetadata serveCategoryBasedProjectJson(@PathParam("projectRowBase64") String projectRowBase64) {
        return getChoiceProjectMetadataEntity(getApiKey(), decode64(projectRowBase64), ALL_METADATA);
    }

    // obtains a project where the category
    // is not known or used
    @GET
    @Path("{projectRowBase64}/files")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectMetadata getAllProjectFileMetadataJson(@PathParam("projectRowBase64") String projectRowBase64) {
        ProjectMetadata rVal = null;
        try {
            long start = System.currentTimeMillis();
            String projectRow = decode64(projectRowBase64);
            rVal = new ProjectMetadata();
            rVal.setUrl(getAbsoluteUrl());
            rVal.setKey(projectRow);
            rVal.setRowId(projectRow);
            rVal.setCodeUrl("N/A");
            rVal.setMetadata(ScanWrapper.readUserDataRecForWholeCF(getApiKey(), decode64(projectRowBase64), PROJECT_FILES_METADATA_CF, null));
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), e.getMessage()).build());
        }
        return rVal;
    }

    @GET
    @Path("{projectRowBase64}/files/{fileHash}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectMetadata getAllFileMetadataJson(@PathParam("projectRowBase64") String projectRowBase64,
                                                  @PathParam("fileHash") String fileHash) {
        ProjectMetadata rVal = null;
        try {
            long start = System.currentTimeMillis();
            rVal = new ProjectMetadata();
            rVal.setUrl(getAbsoluteUrl());

            String apiKey = getApiKey();
            String projectRow = decode64(projectRowBase64);

            rVal.setKey(projectRow);
            rVal.setRowId(projectRow);

            // set up the colQ regex
            String keyToPullRegEx = new StringBuilder(".+::").append(fileHash).append(".+").toString();
            rVal.setMetadata(ScanWrapper.readUserDataRecForWholeCFWithIterator(apiKey, decode64(projectRowBase64), PROJECT_FILES_METADATA, keyToPullRegEx));
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), e.getMessage()).build());
        }
        return rVal;
    }

    @GET
    @Path("{projectRowBase64}/files/{fileHash}/{metadataKeyBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProjectMetadata getFileMetadataValueJson(@PathParam("projectRowBase64") String projectRowBase64,
                                                    @PathParam("fileHash") String fileHash,
                                                    @PathParam("metadataKeyBase64") String metadataKeyBase64) {
        ProjectMetadata rVal = null;
        try {
            long start = System.currentTimeMillis();
            rVal = new ProjectMetadata();
            rVal.setUrl(getAbsoluteUrl());

            String apiKey = getApiKey();
            String projectRow = decode64(projectRowBase64);

            String metadataKey = decode64(metadataKeyBase64);

            rVal.setKey(projectRow);

            // set up the colQ regex
            // metadataKey::fileHash::(alias):<value>
            String keyToPullRegEx = new StringBuilder(metadataKey).append("::").append(fileHash)
                    .append(".+").toString();

            rVal.setMetadata(ScanWrapper.readUserDataRecForWholeCFWithIterator(apiKey, decode64(projectRowBase64), PROJECT_FILES_METADATA, keyToPullRegEx));
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), e.getMessage()).build());
        }
        return rVal;
    }

    @PUT
    @Path("{projectRowBase64}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse putToProjectMetadata(@PathParam("projectRowBase64") String projectRowBase64,
                                             UserDataRecord userDataRecord){
        MuseResponse rVal = null;
        try {
            Security.validateAndCompleteUserDataRecord(getApiKey(),userDataRecord);
            long start = System.currentTimeMillis();
            String projectRow = decode64(projectRowBase64);
            ScanWrapper.updateUserDataRec(getApiKey(), projectRow,
                    ScanWrapper.PROJECT_METADATA_CF, userDataRecord);

            rVal = new MuseResponse();
            rVal.setUrl(getAbsoluteUrl());
            rVal.setKey(userDataRecord.getKey());
            String s = userDataRecord.getValue();
            rVal.setValue(new StringBuilder("(1st 100 Characters)- ").append(s.substring(0, Math.min(s.length(), 100))).toString());
            rVal.setMessage(new StringBuilder("Project metadata updated to: ").append(projectRow).toString());
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), e.getMessage()).build());
        }
        return rVal;
    }

    @PUT
    @Path("{projectRowBase64}/files/{fileHash}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse putToContentMetadata(@PathParam("projectRowBase64") String projectRowBase64,
                                             @PathParam("fileHash") String fileHash,
                                             UserDataRecord userDataRecord){
        MuseResponse rVal = null;
        try {
            String apiKey = getApiKey();
            Security.validateAndCompleteUserDataRecord(apiKey,userDataRecord);
            long start = System.currentTimeMillis();
            String projectRow = decode64(projectRowBase64);

            // for file based metadata the key pattern is:
            // <metadata>::<file hash>
            userDataRecord.setKey(new StringBuilder(userDataRecord.getKey())
                    .append("::").append(fileHash).toString());

            ScanWrapper.updateUserDataRec(apiKey, projectRow,
                    ScanWrapper.PROJECT_FILES_METADATA_CF, userDataRecord);

            rVal = new MuseResponse();
            rVal.setUrl(getAbsoluteUrl());
            rVal.setKey(userDataRecord.getKey());
            String s = userDataRecord.getValue();
            rVal.setValue(new StringBuilder("(1st 100 Characters)- ").append(s.substring(0, Math.min(s.length(), 100))).toString());
            rVal.setMessage(new StringBuilder("Content metadata updated to: ").append(projectRow).toString());
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), e.getMessage()).build());
        }
        return rVal;
    }

    // deletes a key from a project
    // the key can be assigned from the projectMetadata
    // or the content metadata
    @DELETE
    @Path("{projectRowBase64}/{metadataKey64}")
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse deleteContentMetadata(@PathParam("projectRowBase64") String projectRowBase64,
                                              @PathParam("metadataKey64") String metadataKey64){
        MuseResponse rVal = null;
        try {
            long start = System.currentTimeMillis();
            String projectRow = decode64(projectRowBase64);
            String metadataKey = decode64(metadataKey64);
            ScanWrapper.deleteUserDataRow(getApiKey(), projectRow,
                    ScanWrapper.PROJECT_METADATA_CF, metadataKey);

            rVal = new MuseResponse();
            rVal.setUrl(getAbsoluteUrl());
            rVal.setKey(metadataKey);
            rVal.setMessage(new StringBuilder("Project metadata deleted from: ").append(projectRow).toString());
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), e.getMessage()).build());
        }
        return rVal;
    }

    // deletes a key from a project file
    @DELETE
    @Path("{projectRowBase64}/files/{fileHash}/{metadataKeyBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse deleteContentMetadata(@PathParam("projectRowBase64") String projectRowBase64,
                                              @PathParam("fileHash") String fileHash,
                                              @PathParam("metadataKeyBase64") String metadataKeyBase64){
        MuseResponse rVal = null;
        try {
            long start = System.currentTimeMillis();
            String projectRow = decode64(projectRowBase64);

            // for file based metadata the key pattern is:
            // <metadata>::<file hash>
            String metadataKey =  new StringBuilder(decode64(metadataKeyBase64))
                    .append("::").append(fileHash).toString();
            ScanWrapper.deleteUserDataRow(getApiKey(), projectRow,
                    ScanWrapper.PROJECT_FILES_METADATA_CF, metadataKey);

            rVal = new MuseResponse();
            rVal.setUrl(getAbsoluteUrl());
            rVal.setKey(metadataKey);
            rVal.setMessage(new StringBuilder("Metadata data deleted from: ").append(projectRow).toString());
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), e.getMessage()).build());
        }
        return rVal;
    }
}

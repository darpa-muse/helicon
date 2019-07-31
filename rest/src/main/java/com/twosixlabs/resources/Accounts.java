package com.twosixlabs.resources;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.twosixlabs.model.entities.MuseResponse;
import com.twosixlabs.model.entities.User;
import com.twosixlabs.muse_utils.AccountServices;
import com.twosixlabs.muse_utils.App;
import com.twosixlabs.muse_utils.Security;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("muse/admin")
public class Accounts extends ResourceBase {
    @POST
    @Path("accounts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse addAccount(String newAccount){
        MuseResponse rVal = null;
        try {
            if (Security.isAdmin(getApiKey())) {
                long start = System.currentTimeMillis();
                User user = AccountServices.createAccount(newAccount);
                rVal = new MuseResponse();
                rVal.setUrl(getAbsoluteUrl());
                rVal.setKey("New User Details");

                Gson gson = new GsonBuilder().disableHtmlEscaping().create();
                rVal.setValue(gson.toJson(user));
                rVal.setMessage(new StringBuilder("New user created: ")
                        .append(user.getAlias()).toString());
                rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
            }else{
                throw new RuntimeException(ExceptionBase.UNAUTHORIZED_ADMIN_ONLY);
            }
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), msg).build());
        }
        return rVal;
    }

    @GET
    @Path("accounts")
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse getAllAccounts() {
        MuseResponse rVal = null;
        try {
            long start = System.currentTimeMillis();

            rVal = new MuseResponse();
            rVal.setUrl(getAbsoluteUrl());
            rVal.setKey("Existing Accounts");

            List<User> user = AccountServices.getAllAccounts(getApiKey());
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            rVal.setValue(gson.toJson(user));
            rVal.setMessage(new StringBuilder(user.size()).append(" Accounts Returned.").toString());
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), msg).build());
        }
        return rVal;
    }

    @GET
    @Path("accounts/{aliasBased64}")
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse getAccount(@PathParam("aliasBased64") String aliasBased64) {
        MuseResponse rVal = null;
        try { long start = System.currentTimeMillis();

            rVal = new MuseResponse();
            rVal.setUrl(getAbsoluteUrl());
            rVal.setKey("User Details");

            String alias = decode64(aliasBased64);
            User user = AccountServices.getAccount(getApiKey(), alias);
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            rVal.setValue(gson.toJson(user));
            rVal.setMessage(new StringBuilder("User Returned: ")
                    .append(user.getAlias()).toString());
            rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);

        } catch (RuntimeException e) {
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), msg).build());
        }
        return rVal;
    }

    @PUT
    @Path("accounts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse updateAccount(String accountToUpdate){
        MuseResponse rVal = null;
        try {
            String apiKey = getApiKey();
            if (Security.isMuseUser(apiKey)) {
                long start = System.currentTimeMillis();
                User user = AccountServices.updateAccount(apiKey, accountToUpdate);
                rVal = new MuseResponse();
                rVal.setUrl(getAbsoluteUrl());
                rVal.setKey("Updated User Details");

                Gson gson = new GsonBuilder().disableHtmlEscaping().create();
                rVal.setValue(gson.toJson(user));
                rVal.setMessage(new StringBuilder("User updated: ")
                        .append(user.getAlias()).toString());
                rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
            }else{
                throw new RuntimeException(ExceptionBase.UNAUTHORIZED_MUSE_USERS_ONLY);
            }
        }catch (Exception e){
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), msg).build());
        }
        return rVal;
    }

    @DELETE
    @Path("accounts/{aliasBase64}")
    @Produces(MediaType.APPLICATION_JSON)
    public MuseResponse deleteAccount(@PathParam("aliasBase64") String aliasBase64) {
        MuseResponse rVal = null;
        try {
            long start = System.currentTimeMillis();

            if (Security.isAdmin(getApiKey())) {
                String aliasToDelete = decode64(aliasBase64);

                if (!Security.isMuseUser(Security.getApiKey(aliasToDelete))){
                    throw new RuntimeException(ExceptionBase.RESOURCE_NOT_FOUND);
                }

                rVal = new MuseResponse();
                rVal.setUrl(getAbsoluteUrl());
                rVal.setKey("Delete User Details");

                User userDeleted = AccountServices.deleteUser(aliasToDelete);
                rVal.setValue(aliasToDelete);
                rVal.setMessage(new StringBuilder("User Deleted: ")
                        .append(userDeleted.getAlias()).toString());
                rVal.setElapsedTimeMsec(System.currentTimeMillis() - start);
            }else{
                throw new RuntimeException(ExceptionBase.UNAUTHORIZED_ADMIN_ONLY);
            }
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            App.logException(e);
            throw new WebApplicationException(Response.status(App.getStatusCode(msg), msg).build());
        }
        return rVal;
    }
}

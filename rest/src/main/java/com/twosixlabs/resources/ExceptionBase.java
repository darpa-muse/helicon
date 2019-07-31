package com.twosixlabs.resources;

import com.twosixlabs.muse_utils.Security;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class ExceptionBase {
    public static final String UNAUTHORIZED_ADMIN_ONLY = "403:Unauthorized- Administrators Only.";
    public static final String UNAUTHORIZED_ACCOUNT_DISABLED = "403:Unauthorized- Account disabled.";
    public static final String UNAUTHORIZED_MUSE_USERS_ONLY = "403:Unauthorized- MUSE Users Only.";
    public static final String UNAUTHORIZED_FOR_USERDATA_WRITE = "403:Unauthorized for Userdata write.";
    public static final String UNAUTHORIZED_FOR_USER_CHANGE = "403: Unauthorized for changing user accounts";
    public static final String ALIAS_NOT_UNIQUE = "409: Alias is not unique.";
    public static final String RESOURCE_NOT_FOUND = "404: Resource not found.";
    public static final String FAILED_TO_GET_APIKEY = "500: Error getting the api key.";
    public static final String FAILED_TO_GET_ROLE = "500: Error getting the user's role.";
    public static final String TABLE_NOT_FOUND = "500: Table not found.";
    public static final String MUTATION_REJECTED = "500: Error making database table data change.";
    public static final String RESOURCE_CONFLICT = "409: Resource CONFLICT";
    public static final String ERROR_GETTING_PROJECTS_BASED_ON_COLUMN_FAMILY = "500: Error getting projects based on column family.";
    public static final String INVALID_KEY = "422: Unprocessable Entity- Invalid Key";
}

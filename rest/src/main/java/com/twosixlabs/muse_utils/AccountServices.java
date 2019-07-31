package com.twosixlabs.muse_utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.User;
import com.twosixlabs.resources.ExceptionBase;

import java.lang.reflect.Type;
import java.time.YearMonth;
import java.util.List;

public class AccountServices {
    public static User createAccount(String newUserAsString){
        Gson gson = new Gson();
        Type newUserType = new TypeToken<User>() {}.getType();
        User user = gson.fromJson(newUserAsString, newUserType);

        // alias has to be unique
        if (!ScanWrapper.isAliasUnique(user.getAlias())){
            throw new RuntimeException(ExceptionBase.ALIAS_NOT_UNIQUE);
        }

        // set up the defaults
        // if no role - default to user
        String role = user.getRole();
        if (role == null) {
            user.setRole("user");
        }

        user.setCreated(YearMonth.now());

        // set a two year expiration
        YearMonth expiration = YearMonth.of(YearMonth.now().getYear() + 2, YearMonth.now().getMonth());
        user.setExpires(expiration);

        // go ahead and add it to the db
        return ScanWrapper.newUser(user);
    }

    // read an account (owners and admins can do this)
    public static List<User> getAllAccounts(String apiKey){
        List<User> rVal = null;

        if (Security.isAdmin(apiKey)){
            rVal = ScanWrapper.getAllAccounts();
        }else{
            throw new RuntimeException(ExceptionBase.UNAUTHORIZED_ADMIN_ONLY);
        }
        return rVal;
    }

    // read an account (owners and admins can do this)
    public static User getAccount(String apiKey, String alias){
        User rVal = null;
        String callerAlias = Security.getAlias(apiKey);
        if (apiKey != null && callerAlias != null && (Security.isAdmin(apiKey) || callerAlias.equalsIgnoreCase(alias))){
            rVal = ScanWrapper.getAccount(alias);
        }else{
            throw new RuntimeException(ExceptionBase.UNAUTHORIZED_ADMIN_ONLY);
        }
        return rVal;
    }

    // only updates read and write peers
    public static User updateAccount(String apiKey, String newUserAsString){
        User rVal = null;

        Gson gson = new Gson();
        Type newUserType = new TypeToken<User>() {}.getType();
        User user = gson.fromJson(newUserAsString, newUserType);
        String alias = Security.getAlias(apiKey);
        String userAlias = user.getAlias();

        // Two cases:
        // 1) Admin modifying an account
        // 2) Admin or user modifying its own account
        // set up user record
        if (userAlias == null){
            userAlias = alias;
            user.setAlias(alias);
        }

        user.setApiKey(Security.getApiKey(userAlias));
        if (userAlias.equalsIgnoreCase(alias)
                || Security.isAdmin(apiKey)){
            // update
            rVal = ScanWrapper.updateUser(user);
        }else{
            throw new RuntimeException(ExceptionBase.UNAUTHORIZED_FOR_USER_CHANGE);
        }
        return rVal;
    }

    public static User deleteUser(String aliasToDelete){
        User userToDelete = new User();

        String apiKeyToDelete = Security.getApiKey(aliasToDelete);
        String roleToDelete = Security.getRole(aliasToDelete, apiKeyToDelete);

        userToDelete.setAlias(aliasToDelete);
        userToDelete.setApiKey(apiKeyToDelete);
        userToDelete.setRole(roleToDelete);

        ScanWrapper.deleteUser(userToDelete);
        return userToDelete;
     }
}

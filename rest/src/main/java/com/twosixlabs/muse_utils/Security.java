package com.twosixlabs.muse_utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.UserDataRecord;
import com.twosixlabs.resources.ExceptionBase;
import com.twosixlabs.resources.ResourceBase;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;

import static com.twosixlabs.model.accumulo.ScanWrapper.MUSE_USERS_TABLE;

public class Security {

/*
Security scenarios:
 - UserDataRecord:
 {
    owner(optional): <specify caller's alias or target alias if changing the record>
    readPeers(optional): <specify a list of aliases that can read this record>
    writePeers(optional): <specify a list of aliases that can change the value of this record>
    key(required): <any valid string>
    value(required): <any valid string>
 }

*/

    // fill fields that are not required but make processing easier
    public static UserDataRecord validateAndCompleteUserDataRecord(String apiKey,
                                                                   UserDataRecord userDataRecord) {
        UserDataRecord completeUserDataRecord = userDataRecord;
        isUserDataRecordValid(userDataRecord);

        if (userDataRecord.getOwner() == null) {
            completeUserDataRecord.setOwner(getAlias(apiKey));
        }
        return completeUserDataRecord;
    }

    public static boolean isUserDataRecordValid(UserDataRecord userDataRecord) {
        boolean valid = false;
        if (userDataRecord != null
                && userDataRecord.getValue() != null
                && userDataRecord.getKey() != null) {
            valid = true;
        } else {
            throw new RuntimeException("400");
        }
        return valid;
    }

    private static Text DISABLED_CF = new Text("disabled");

    public static void disableAccount(String apiKey){
        ScanWrapper.insert(MUSE_USERS_TABLE, apiKey, DISABLED_CF, new Text(""), new Value(""));
    }
    public static void enableAccount(String apiKey){
        ScanWrapper.deleteRecord(MUSE_USERS_TABLE, apiKey, DISABLED_CF, null);
    }
    // checks if there is a disabled account
    public static boolean isDisabled(String apiKey) {
        boolean isDisabled = false;
        // check if a key exists;
        if (apiKey != null) {
            try {
                Scanner scanner = ScanWrapper.getScanner(MUSE_USERS_TABLE);
                scanner.setRange(Range.exact(new Text(apiKey)));
                scanner.fetchColumnFamily(DISABLED_CF);
                Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
                if (it.hasNext()) {
                    isDisabled = true;
                }
            } catch (Exception e) {
                App.logException(e);
                throw e;
            }
        }
        return isDisabled;
    }

    // checks if there is a valid key
    public static boolean isMuseUser(String apiKey) {
        boolean authenticated = false;
        // check if a key exists;
        if (apiKey != null) {
            try {
                Scanner scanner = ScanWrapper.getScanner(MUSE_USERS_TABLE);
                scanner.setRange(Range.exact(new Text(apiKey)));
                Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
                if (it.hasNext()){
                    if (!isDisabled(apiKey)) {
                        authenticated = true;
                    }else{
                        throw new RuntimeException(ExceptionBase.UNAUTHORIZED_ACCOUNT_DISABLED);
                    }
                } else {
                    throw new RuntimeException(ExceptionBase.UNAUTHORIZED_MUSE_USERS_ONLY);
                }
            } catch (Exception e) {
                App.logException(e);
                throw e;
            }
        }
        return authenticated;
    }
/*
              Row      CF:CQ            VALUE
             <key>  role:alias          <alias>
             <key>  role:readPeers      <readPeer list>
             <key>  role:writePeers     <writePeer list>
             <role> alias=<alias>:<key>     []
*/
public static String getRole(String alias, String apiKey){
    String role = null;

    try {
        Scanner scanner = ScanWrapper.getScanner(MUSE_USERS_TABLE);
        //scanner.setRange(Range.exact(new Text(apiKey)));
        Text cf = new Text(new StringBuilder("alias=").append(alias).toString());
        Text cq = new Text(apiKey);
        scanner.fetchColumn(cf, cq);
        Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
        while (it.hasNext()) {
            Map.Entry<Key, Value> e = it.next();
            role = e.getKey().getRow().toString();
        }
    } catch (Exception e) {
        App.logException(e);
        throw new RuntimeException(ExceptionBase.FAILED_TO_GET_ROLE);
    }

    return role;
}

    public static String getApiKey(String alias){
        String apiKey = null;

        try {
            Scanner scanner = ScanWrapper.getScanner(MUSE_USERS_TABLE);
            //scanner.setRange(Range.exact(new Text(apiKey)));
            Text cf = new Text(new StringBuilder("alias=").append(alias).toString());
            scanner.fetchColumnFamily(cf);
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()) {
                Map.Entry<Key, Value> e = it.next();
                apiKey = e.getKey().getColumnQualifier().toString();
            }
        } catch (Exception e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.FAILED_TO_GET_APIKEY);
        }

        return apiKey;
    }

    public static String getAlias(String apiKey) {
        String alias = null;

        try {
            Scanner scanner = ScanWrapper.getScanner(MUSE_USERS_TABLE);
            scanner.setRange(Range.exact(new Text(apiKey)));
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()) {
                Map.Entry<Key, Value> e = it.next();
                String cq = e.getKey().getColumnQualifier().toString();
                if (cq.equalsIgnoreCase("alias")) {
                    alias = e.getValue().toString();
                }
            }
        } catch (Exception e) {
            App.logException(e);
        }

        return alias;
    }

    public static Text WRITE_PEERS_CQ = new Text("writePeers");
    public static String WRITE_PEERS = WRITE_PEERS_CQ.toString();
    public static Text READ_PEERS_CQ = new Text("readPeers");
    public static String READ_PEERS = READ_PEERS_CQ.toString();

    private static ArrayList<String> getWritePeersFromAlias(String alias) {
        return getPeersFromAlias(alias, WRITE_PEERS);
    }

    private static Text USER_CF = new Text("user");
    private static Text ADMIN_CF = new Text("admin");

    private static ArrayList<String> getReadPeersFromAlias(String alias) {
        return getPeersFromAlias(alias, READ_PEERS);
    }

    private static ArrayList<String> getPeersFromAlias(String alias, String type) {
        ArrayList<String> peers = null;
        Text peerType = type.equalsIgnoreCase(WRITE_PEERS) ? WRITE_PEERS_CQ : READ_PEERS_CQ;
        try {
            Type listType = new TypeToken<List<String>>() {
            }.getType();
            Gson gson = new Gson();
            Scanner scanner = ScanWrapper.getScanner(MUSE_USERS_TABLE);
            scanner.fetchColumnFamily(new Text(new StringBuilder("alias=").append(alias).toString()));
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            // alias --> apiKey(cq),role(cf) -->row:apiKey cf:role cq:read/writePeers value:[<peer1>, <peer2>, ..., <peern>]
            String apiKey = null;
            Text role = null;
            if (it.hasNext()) {
                Map.Entry<Key, Value> e = it.next();
                apiKey = e.getKey().getColumnQualifier().toString();
                role = e.getKey().getRow();
            }
            scanner.clearColumns();
            scanner.setRange(Range.exact(new Text(apiKey)));
            scanner.fetchColumn(role, peerType);
            it = scanner.iterator();
            if (it.hasNext()) {
                Map.Entry<Key, Value> e = it.next();
                peers = gson.fromJson(new String(e.getValue().get()), listType);
            }
        } catch (Exception e) {
            App.logException(e);
        }
        return peers != null ? peers : new ArrayList<String>();
    }


    private static final String ANYONE = "any";
    private static final String ADMIN_ROLE = "admin";

    public static boolean isAdmin(String apiKey) {
        boolean isAdmin = false;
        try {
            Scanner scanner = ScanWrapper.getScanner(MUSE_USERS_TABLE);
            scanner.setRange(Range.exact(new Text(apiKey)));
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()) {
                Map.Entry<Key, Value> e = it.next();
                if (e.getKey().getColumnFamily().toString().equalsIgnoreCase(ADMIN_ROLE)) {
                    isAdmin = true;
                    break;
                }
            }
        } catch (Exception e) {
            App.logException(e);
        }
        return isAdmin;
    }
    public static boolean isAuthorizedForDelete(String apiKey, UserDataRecord userDataRecord) {
        boolean authorized = false;
        if (isMuseUser(apiKey)){
            if (0 == getAlias(apiKey).compareToIgnoreCase(userDataRecord.getOwner())){
                authorized = true;
            }
        }
        return authorized;
    }
    public static boolean isAuthorizedForWrite(String apiKey, UserDataRecord userDataRecord) {
        boolean authorized;

        if (userDataRecord == null && isMuseUser(apiKey)){
            authorized = true;
        }else{
            List<String> writePeers = userDataRecord.getWritePeers();
            boolean rb1 = writePeers != null ? writePeers.contains(ANYONE) : false;
            boolean rb2 = writePeers != null ? writePeers.contains(getAlias(apiKey)) : false;

            authorized = (rb1 || rb2
                    || isAdmin(apiKey)
                    || isAuthorizedForReadWrite(apiKey, userDataRecord, WRITE_PEERS));
        }

        return authorized;
    }

    public static boolean isAuthorizedForRead(String apiKey, UserDataRecord userDataRecord) {
        List<String> readPeers = userDataRecord.getReadPeers();
        boolean rb1 = readPeers != null? readPeers.contains(ANYONE):false;
        boolean rb2 = readPeers != null? readPeers.contains(getAlias(apiKey)):false;



        boolean authorized = (rb1 || rb2
                || isAdmin(apiKey)
                || isAuthorizedForReadWrite(apiKey, userDataRecord, READ_PEERS));

        return authorized;
    }

    public static boolean isAuthorizedForReadWrite(String apiKey, UserDataRecord userDataRecord, String peerType){
        boolean isAuthorizedForReadWrite = false;
        try{
            if (!isMuseUser(apiKey)){
                throw new RuntimeException(ExceptionBase.UNAUTHORIZED_MUSE_USERS_ONLY);
            }
            Function<String,ArrayList<String>> getPeersFromAliasFnc = null;
            getPeersFromAliasFnc = peerType.equalsIgnoreCase(WRITE_PEERS)?
                    Security::getWritePeersFromAlias: Security::getReadPeersFromAlias;
            String alias = getAlias(apiKey);
            // 1. if the alias == owner we are good
            // 2. if the owner has the apiKey alias listed in its write/READ_peers
            if (alias.equalsIgnoreCase(userDataRecord.getOwner())
                    || getPeersFromAliasFnc.apply(userDataRecord.getOwner()).contains(alias)
                    || getPeersFromAliasFnc.apply(userDataRecord.getOwner()).contains(ANYONE)){
                isAuthorizedForReadWrite = true;
            }
        }catch(Exception e){
            App.logException(e);
            throw e;
        }
        return isAuthorizedForReadWrite;
    }

    public static void initialize() {

    }

    private static boolean isApiKeyUnique(String apiKey) {
        boolean validKey = false;
        // check if a key exists;
        if (apiKey != null) {
            try {
                Scanner scanner = ScanWrapper.getScanner(MUSE_USERS_TABLE);
                scanner.setRange(Range.exact(new Text(apiKey)));
                Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
                if (it.hasNext()){
                    validKey = true;
                }
            } catch (Exception e) {
                App.logException(e);
                throw e;
            }
        }
        return validKey;
    }

    private static final int KEY_ARRAY_LENGTH = 256;
    public static void testForValidKey(String apiKey) {
        // simple validity test
        if (Security.isApiKeyUnique(apiKey) || Base64.getDecoder().decode(apiKey).length != 256){
            throw new RuntimeException(ExceptionBase.INVALID_KEY);
        }
    }
}

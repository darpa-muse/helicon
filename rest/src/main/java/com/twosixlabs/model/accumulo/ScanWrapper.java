package com.twosixlabs.model.accumulo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.twosixlabs.model.entities.MetadataValueStats;
import com.twosixlabs.model.entities.User;
import com.twosixlabs.model.entities.UserDataRecord;
import com.twosixlabs.muse_utils.App;
import com.twosixlabs.muse_utils.Security;
import com.twosixlabs.resources.ExceptionBase;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.lang.reflect.Type;
import java.security.SecureRandom;
import java.time.YearMonth;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ScanWrapper {
    private static final Logger logger = Logger.getLogger(ScanWrapper.class.getName());

    private static final Text PROJECT_SIZE_STATS_CQ = new Text("Project Code Size");
    private static final String PROJECT_SIZE_STATS = PROJECT_SIZE_STATS_CQ.toString();
    private static final Text PROJECT_FORKS_STATS_CQ = new Text("Forks");
    private static final String PROJECT_FORKS_STATS = PROJECT_FORKS_STATS_CQ.toString();
    private static final Text PROJECT_WATCHERS_STATS_CQ = new Text("Watchers");
    private static final String PROJECT_WATCHERS_STATS = PROJECT_WATCHERS_STATS_CQ.toString();
    private static final Text PROJECT_STARGAZERS_STATS_CQ = new Text("Stargazers");
    private static final String PROJECT_STARGAZERS_STATS = PROJECT_STARGAZERS_STATS_CQ.toString();
    private static final Text LANGUAGES_CQ = new Text("languages");
    private static final Text TOPICS_CQ = new Text("topics");
    private static final Text PROJECT_METADATA_KEYS_CQ = new Text("projectMetadataKeys");
    private static final String PROJECT_METADATA_KEYS = PROJECT_METADATA_KEYS_CQ.toString();

    private static final Text SUMMARY_STATS_CF = new Text("stats");
    private static final Text PROJECT_COUNT_CQ = new Text("Projects");
    private static final String PROJECT_COUNT = PROJECT_COUNT_CQ.toString();
    private static final Text CATEGORY_CF = new Text("category");
    private static final Text SUMMARY_HELPER_CF = new Text("helper");
    private static final String SUMMARY_TABLE = "muse_summary";
    private static final String MUSE_CORPUS = "muse_corpus";
    private static final Text PROJECT_METADATA_COLFAM = new Text("projectMetadata");
    private static final Text PROJECT_METADATA_COLQ_CODE = new Text("code");
    private static final Text CORPUS_AUTOSUGGEST_VALUES_CQ = new Text("corpusAutocorrectValues");
    private static final String TOTALS = "totals";
    private static final Text TOTALS_ROW = new Text(TOTALS);
    public static final String ALL_METADATA = "ALL_METADATA";
    public static final Text PROJECT_METADATA_CF = new Text("projectMetadata");
    public static final String PROJECT_METADATA = PROJECT_METADATA_CF.toString();
    public static final Text PROJECT_FILES_METADATA_CF = new Text("projectFilesMetadata");
    public static final String PROJECT_FILES_METADATA = PROJECT_FILES_METADATA_CF.toString();
    public static final String MUSE_USER_DATA_TABLE = "muse_user_data";
    public static final String MUSE_USERS_TABLE = "muse_users";

    public static final int PAGE_SIZE = 25;

    public static String ZOOKEEPER_PORT = App.ZOOKEEPER_PROD_PORT;
    private static FileHandler fh;

    private static final Text CORPUS_VERSION_ROW_TITLE = new Text("corpusVersion");
    private static String corpusVersion;
    private static final Text EMPTY_TEXT = new Text("");

    private static final Range CORPUS_RANGE = new Range("5.0|", "5.9|");
    public static MetadataValueStats crank() {
        MetadataValueStats stats;
        Scanner scan = getScanner(MUSE_CORPUS);
        scan.setRange(CORPUS_RANGE);
        scan.fetchColumnFamily(PROJECT_METADATA_COLFAM);
        ArrayList<Double> keyCounts = new ArrayList<>();
        Iterator<Map.Entry<Key, Value>> projIterator = scan.iterator();
        while (projIterator.hasNext()) {
            double keyCount = 0;
            Text row = projIterator.next().getKey().getRow();
            Text nextRow = row;
            while (projIterator.hasNext() && nextRow.equals(row)) {
                keyCount++;
                nextRow =projIterator.next().getKey().getRow();
            }
            keyCounts.add(keyCount);
        }
        final double[] result = new double[keyCounts.size()];
        int i = 0;
        for (Double d : keyCounts) {
            result[i++] = d.doubleValue();
        }

        return new MetadataValueStats(result);
    }

    public static Iterator<Map.Entry<Key, Value>> getProjectMetadataIterator(String project) {
        return  getProjectMetadataIterator(new Text(project));
    }

    public static Iterator<Map.Entry<Key, Value>> getProjectMetadataIterator(Text project) {
        Scanner scan = getScanner(MUSE_CORPUS);
        scan.setRange(Range.exact(project));
        scan.fetchColumnFamily(PROJECT_METADATA_COLFAM);

        Iterator<Map.Entry<Key, Value>> rVal = scan.iterator();
        scan.close();
        return rVal;
    }

    public static Iterator<Map.Entry<Key, Value>> getProjectFilesMetadataIterator(String project) {
        return  getProjectFilesMetadataIterator(new Text(project));
    }

    public static Iterator<Map.Entry<Key, Value>> getUserProjectFilesMetadataIterator(Text project) {
        Scanner scan = getScanner(MUSE_USER_DATA_TABLE);
        scan.setRange(Range.exact(project));
        scan.fetchColumnFamily(PROJECT_FILES_METADATA_CF);

        Iterator<Map.Entry<Key, Value>> rVal = scan.iterator();
        scan.close();
        return rVal;
    }
    public static Iterator<Map.Entry<Key, Value>> getProjectFilesMetadataIterator(Text project) {
        Scanner scan = getScanner(MUSE_CORPUS);
        scan.setRange(Range.exact(project));
        scan.fetchColumnFamily(PROJECT_FILES_METADATA_CF);

        Iterator<Map.Entry<Key, Value>> rVal = scan.iterator();
        scan.close();
        return rVal;
    }

    public static String getProjectCodeUrl(String project) {
        String rVal = "";
        Scanner scan = getScanner(MUSE_CORPUS);
        scan.setRange(Range.exact(new Text(project)));
        scan.fetchColumn(PROJECT_METADATA_COLFAM, PROJECT_METADATA_COLQ_CODE);

        Iterator<Map.Entry<Key, Value>> scanResults = scan.iterator();
        if (scanResults.hasNext()) {
            rVal = scanResults.next().getValue().toString();
        }

        scan.close();
        return rVal;
    }

    public static HashSet<Text> getProjectsBasedOnColFam(String colFam, String prefix, int page) {
        return getProjectsBasedOnColFam(colFam, prefix, PAGE_SIZE, page, true);
    }

    public static HashSet<Text> getProjectsBasedOnColFam(String colFam, String prefix, int pageSize, int page, boolean cache) {
        String cacheKey = new StringBuilder("getProjectsBasedOnColFam").append(colFam)
                .append("pagesize=").append(pageSize).append("page=").append(page).toString();
        HashSet<Text> projects = !cache? null :
                (HashSet<Text>) App.resultsCache.get(cacheKey);
        try {
            if (projects == null) {
                projects = new HashSet<>();
                Scanner scan = getScanner(MUSE_CORPUS);

                int count = 0;
                scan.setRange(getRange(prefix));

                if (page > 0 && pageSize > 0)
                    scan.setBatchSize(page * pageSize);

                scan.fetchColumnFamily(new Text(colFam));
                Iterator<Map.Entry<Key, Value>> scanResults = scan.iterator();
                Map.Entry<Key, Value> e = null;
                if (page > 1) {
                    while (scanResults.hasNext() && ++count < page * pageSize) {
                        scanResults.next();
                    }
                }
                while (scanResults.hasNext()) {
                    e = scanResults.next();
                    Text cq = e.getKey().getColumnQualifier(); // this is the rowID for the project as part of the partition index
                    projects.add(cq);
                    if (pageSize > 0 && projects.size() % pageSize == 0) {
                        break;
                    }
                }
                App.resultsCache.put(cacheKey, projects);
            }
        }catch (Exception e){
            App.logException(e);
            throw new RuntimeException(ExceptionBase.ERROR_GETTING_PROJECTS_BASED_ON_COLUMN_FAMILY);
        }
        return projects;
    }

    public static HashMap<String, String> getProjectsWithMetadataKey(String category, int pageSize, int page, String metadataKey, boolean cache) {
        String cacheKey = new StringBuilder("getProjectsWithMetadataKey").append(category)
                .append(pageSize).append(page).append(metadataKey).toString();
        HashMap<String, String> projects = !cache? null :
                (HashMap<String, String>) App.resultsCache.get(cacheKey);

        if (projects == null) {
            projects = new HashMap<>();
            try {
                int count = 0;
                Scanner scanner = getScanner(MUSE_CORPUS);
                scanner.setBatchSize(page * pageSize);
                scanner.fetchColumn(PROJECT_METADATA_COLFAM, new Text(category));
                scanner.setRange(Range.prefix("5.0"));

                Iterator<Map.Entry<Key, Value>> scanResults = scanner.iterator();
                if (page > 1) {
                    while (scanResults.hasNext() && count++ < (page - 1) * pageSize) {
                        scanResults.next();
                    }
                }
                while (scanResults.hasNext()) {
                    Map.Entry<Key, Value> e = scanResults.next();
                    Text projectRow = e.getKey().getRow(); // this is the rowID for the project as part of the partition index
                    projects.put(projectRow.toString(), getProjectMetadataItem(projectRow.toString(), metadataKey));

                    if (projects.size() > 0 && projects.size() % pageSize == 0) {
                        break;
                    }
                }
                App.resultsCache.put(cacheKey, projects);
            }catch (Exception e){
                App.logException(e);
            }
        }
        return projects;
    }

    public static HashMap<String, String> getProjectsBasedOnColFamAndMetadataKey(String colFam, String prefix, int pageSize, int page, String metadataKey,
                                                                                 boolean cache) {
        String cacheKey = new StringBuilder("getProjectsBasedOnColFamAndMetadataKey").append(colFam)
                .append(prefix).append(pageSize).append(page).append(metadataKey).toString();
        HashMap<String, String> projects = !cache? null :
                (HashMap<String, String>) App.resultsCache.get(cacheKey);

        if (projects == null) {
            try {
                projects = new HashMap<>();
                Scanner scan = getScanner(MUSE_CORPUS);

                int count = 0;
                char lastChar = (prefix.charAt(prefix.length() - 1));
                lastChar++;
                scan.setRange(new Range(prefix, true, prefix.substring(0, prefix.length() - 1) + lastChar, true));
                //scan.setRange( Range.prefix(prefix));
                scan.setBatchSize(page * pageSize);

                scan.fetchColumnFamily(new Text(colFam));
                Iterator<Map.Entry<Key, Value>> scanResults = scan.iterator();

                Map.Entry<Key, Value> e = null;
                if (page > 1) {
                    while (scanResults.hasNext() && count++ < (page - 1) * pageSize) {
                        scanResults.next();
                    }
                }
                while (scanResults.hasNext()) {
                    e = scanResults.next();
                    Text projectRow = e.getKey().getColumnQualifier(); // this is the rowID for the project as part of the partition index
                    projects.put(projectRow.toString(), getProjectMetadataItem(projectRow.toString(), metadataKey));

                    if (projects.size() % pageSize == 0) {
                        break;
                    }
                }
                App.resultsCache.put(cacheKey,projects);
            }catch (Exception e){
                App.logException(e);
            }
        }
        return projects;
    }

    public static ArrayList<Value> getProjectMetadata(String row, String metadataColQualifier, Long pageSize, Long page, boolean cache) {

        Scanner scan = getScanner(MUSE_CORPUS);
        if (row != null) {
            scan.setRange(getRange(row));
        }
        long count = 0;
        scan.fetchColumn(PROJECT_METADATA_COLFAM, new Text(metadataColQualifier));
        Iterator<Map.Entry<Key, Value>> scanResults = scan.iterator();
        Map.Entry<Key, Value> e = null;

        ArrayList<Value> projectMetadataVals = new ArrayList<>();
        if (page > 1) {
            while (scanResults.hasNext() && ++count < (page - 1) * pageSize) {
                scanResults.next();
            }
        }
        while (scanResults.hasNext()) {
            e = scanResults.next();
            Value v = e.getValue();
            projectMetadataVals.add(v);
            if (++count % pageSize == 0) {
                break;
            }
        }

        scan.close();
        return projectMetadataVals;
    }


    public static UserDataRecord getUserDataRecord(String table, String row, Text colF, String colQ) {
        Iterator<Map.Entry<Key, Value>> iter = null;
        UserDataRecord rVal = null;
        try {
            Scanner scan = getScanner(table);
            scan.setRange(Range.exact(new Text(row)));
            scan.fetchColumn(colF, new Text(colQ));
            iter = scan.iterator();
            if (iter.hasNext()) {
                Gson gson = new Gson();
                Type userDataRecordType = new TypeToken<UserDataRecord>(){}.getType();
                rVal = gson.fromJson(new String(iter.next().getValue().get()), userDataRecordType);
            }
        } catch (Exception e) {
            App.logException(e);
            throw new RuntimeException("500: Error getting user data record");
        }
        return rVal;
    }

    // cf name -> stats map returned...
    public static HashMap<String, MetadataValueStats> getSummaryTableStatsCf() {
        HashMap<String, MetadataValueStats> rVal = new HashMap<>();
        Gson gson = new Gson();
        Type statsType = new TypeToken<MetadataValueStats>(){}.getType();
        try {
            Scanner scan = getScanner(SUMMARY_TABLE);
            scan.setRange(Range.exact(TOTALS_ROW));
            scan.fetchColumnFamily(SUMMARY_STATS_CF);
            if (scan != null) {
                Iterator<Map.Entry<Key, Value>> it = scan.iterator();
                while (it.hasNext()) {
                    Map.Entry<Key, Value> e = it.next();
                    String cq = e.getKey().getColumnQualifier().toString();
                    if (!cq.equalsIgnoreCase(PROJECT_COUNT)) {
                        rVal.put(cq, gson.fromJson(new String(e.getValue().get()), statsType));
                    }
                }
            }
        } catch (Exception e) {
            App.logException(e);
            throw new RuntimeException("500: Problem getting summary stats");
        }
        return rVal;
    }

    public static HashMap<String, ConcurrentHashMap<String, Integer>> getSummaryTableCategoryCf() {
        HashMap<String, ConcurrentHashMap<String, Integer>> rVal = new HashMap<>();
        try {
            Gson gson = new Gson();
            Type categoryType = new TypeToken<ConcurrentHashMap<String, Integer>>(){}.getType();
            Scanner scan = getScanner(SUMMARY_TABLE);
            scan.setRange(Range.exact(TOTALS_ROW));
            scan.fetchColumnFamily(CATEGORY_CF);
            if (scan != null) {
                Iterator<Map.Entry<Key, Value>> it = scan.iterator();
                while(it.hasNext()){
                    Map.Entry<Key, Value> e = it.next();
                    rVal.put(e.getKey().getColumnQualifier().toString(),gson.fromJson(new String(e.getValue().get()), categoryType));
                }
            }
        } catch (Exception e) {
            logger.log(Level.INFO, "Exception!", e);
        }
        return rVal;
    }

    // scan for a cq from the summary table
    // categories are HashMap<String, Integer>
    public static ConcurrentHashMap<String, Integer> getSummaryTotal(String colQual) {
        ConcurrentHashMap<String, Integer> rVal = null;
        try {
            Scanner scan = getScanner(SUMMARY_TABLE);

            if (scan != null) {
                scan.fetchColumn(CATEGORY_CF, new Text(colQual));
                scan.setRange(Range.exact(TOTALS_ROW));
                Gson gson = new Gson();
                Type mapType = new TypeToken<ConcurrentHashMap<String, Integer>>(){}.getType();

                Iterator<Map.Entry<Key, Value>> iter = scan.iterator();
                try {
                    while (iter.hasNext()) {
                        Map.Entry<Key, Value> e = iter.next();

                        rVal = gson.fromJson(new String(e.getValue().get()), mapType);
                    }
                } catch (Exception e) {
                    logger.log(Level.INFO, "Exception!", e);
                }
            }
        } catch (Exception e) {
            logger.log(Level.INFO, "Exception!", e);
        }
        return rVal;
    }

    private static Scanner scanner = null;
    private static Connector connector = null;

    private static Connector getConnector() {
        try {
            if (connector == null) {
                // All of this is subject to big changes
                // when this design supports multi-tennant
                String instanceName = App.getName();
                String zooServers = new StringBuilder(App.getLocation())
                        .append(":").append(App.getPort()).toString();

                ZooKeeperInstance inst = new ZooKeeperInstance(instanceName, zooServers);

                String principal = App.getUser();
                PasswordToken authToken = new PasswordToken(App.getPass());

                connector = inst.getConnector(principal, authToken);
            }
        } catch (AccumuloException e) {
            App.logException(e);
        } catch (AccumuloSecurityException e) {
            App.logException(e);
        }

        return connector;
    }

    public static Scanner getScanner(String table) {
        try {
            Connector conn = getConnector();
            scanner = conn.createScanner(table, new Authorizations());
        } catch (TableNotFoundException e) {
            logger.log(Level.INFO, "Exception!", e);
        }

        return scanner;
    }

    private static BatchScanner getBatchScanner(String table) {
        BatchScanner bs = null;
        try {
            Connector conn = getConnector();
            bs = conn.createBatchScanner(table, new Authorizations(), 4);
        } catch (TableNotFoundException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.TABLE_NOT_FOUND);
        }

        return bs;
    }

    public static String getProjectMetadataItem(String project, String metadataKey) {
        return getValue(MUSE_CORPUS, PROJECT_METADATA_COLFAM, new Text(metadataKey), new Text(project), true);
    }

    private static String GET_VALUE_METHOD = "getValue";

    public static HashMap<String, String> getProjectMetadataFromUserData(String apiKey,
                                                                         String projectRow){
        HashMap<String, String> rVal = null;
        try {
            rVal =  readUserDataRecForWholeCF(apiKey, projectRow, PROJECT_METADATA_CF, null );
        } catch (Exception e) {
            App.logException(e);
        }
        return rVal;
    }

    public static HashMap<String, String> readUserDataRecForWholeCF(String apiKey, String projectRow, Text colFam, Text colq) throws Exception {
        // make sure the alias can read
        HashMap<String, String> rVal = new HashMap<>();
        try{
            Gson gson = new Gson();
            Type userDataRecordType = new TypeToken<UserDataRecord>(){}.getType();

            Scanner scanner = getScanner(MUSE_USER_DATA_TABLE);
            scanner.setRange(Range.exact(new Text(projectRow)));

            if (colq != null) {
                scanner.fetchColumn(colFam, colq);
            }else{
                scanner.fetchColumnFamily(colFam);
            }

            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()){
                Map.Entry<Key, Value> e = it.next();
                String colqStr = e.getKey().getColumnQualifier().toString();
                UserDataRecord userDataRecord = gson.fromJson(new String(e.getValue().get()), userDataRecordType);
                if (Security.isAuthorizedForRead(apiKey, userDataRecord)) {
                    rVal.put(colqStr, userDataRecord.getValue());
                }
            }
        }catch(Exception e){
            App.logException(e);
        }
        return rVal;
    }

    // getIteratorBasedOnRegexFilterAndColFam
    public static HashMap<String, String> readUserDataRecForWholeCFWithIterator(String apiKey, String projectRow, String colFam, String colq) throws Exception {
        // make sure the alias can read
        HashMap<String, String> rVal = new HashMap<>();
        try{
            Gson gson = new Gson();
            Type userDataRecordType = new TypeToken<UserDataRecord>(){}.getType();

            /*
            String rowRegEx,
            String colqRegEx,
            String valRegex,
            String type
             */
            Iterator<Map.Entry<Key, Value>> it = getIteratorBasedOnRegexFilterAndColFam(
                    projectRow,
                    colq,
                    null,
                    colFam
            );
            while (it.hasNext()){
                Map.Entry<Key, Value> e = it.next();
                String colqStr = e.getKey().getColumnQualifier().toString();
                UserDataRecord userDataRecord = gson.fromJson(new String(e.getValue().get()), userDataRecordType);
                if (Security.isAuthorizedForRead(apiKey, userDataRecord)) {
                    rVal.put(colqStr, userDataRecord.getValue());
                }
            }
        }catch(Exception e){
            App.logException(e);
        }
        return rVal;
    }


    private static String getValue(String table, Text colFam, Text colQ, Text exactRange, boolean cache) {

        String rVal = null;
        try {
            String cacheKey = new StringBuilder(GET_VALUE_METHOD).append(table)
                    .append(colFam == null ? null : colFam.toString()).append(colQ == null ? null : colQ.toString()).append(exactRange.toString()).toString();
            rVal = !cache ? null :
                    (String) App.resultsCache.get(cacheKey);
            if (rVal == null) {
                rVal= "";
                Scanner scan = getScanner(table);
                scan.setRange(Range.exact(exactRange));

                if (colFam != null && colQ != null) {
                    scan.fetchColumn(colFam, colQ);
                }
                Iterator<Map.Entry<Key, Value>> scanResults = scan.iterator();
                if (scanResults.hasNext()) {
                    rVal = scanResults.next().getValue().toString();
                }
                App.resultsCache.put(cacheKey, rVal);
                scan.close();
            }
        }catch (Exception e) {
            App.logException(e);
        }

        return rVal;
    }


    public static void initialize() {
        // get the current version

        corpusVersion = getValue(SUMMARY_TABLE, null, null, CORPUS_VERSION_ROW_TITLE, true);
        loadStats();
    }

    public static void initialize(String version, String port) {
        ZOOKEEPER_PORT = port;
        corpusVersion = version;
    }

    public static void loadStats() {
        Connector connector = getConnector();

        try {
            // Helper values
            Gson gson = new Gson();
            Type hashSetType = new TypeToken<HashSet<String>>(){}.getType();

            String corpusAutosuggestValues = getValue(SUMMARY_TABLE, SUMMARY_HELPER_CF, CORPUS_AUTOSUGGEST_VALUES_CQ, TOTALS_ROW, false);
            HashSet<String> strings = gson.fromJson(corpusAutosuggestValues, hashSetType);
            strings.forEach(s -> new StringBuilder("\"").append(s).append("\""));
            App.setCorpusAutosuggestValues(new StringBuilder("\"")
                    .append(String.join("\",\"",strings)).append("\"").toString());

            // Summary column family Stats
            HashMap<String, MetadataValueStats> stats = getSummaryTableStatsCf();
            stats.forEach((k,v) -> {
                if (k.contains(PROJECT_FORKS_STATS)) {
                    App.setForksMetaStats(v);
                } else if (k.contains(PROJECT_WATCHERS_STATS)) {
                    App.setWatchersMetaStats(v);
                } else if (k.contains(PROJECT_SIZE_STATS)) {
                    App.setProjMetaStats(v);
                } else if (k.contains(PROJECT_STARGAZERS_STATS)) {
                    App.setStargazersStats(v);
                }
            });
        } catch (Exception e) {
            App.logException(e);
        }
    }

    public static void writeCategoriesToSummaryTable(String row, HashMap<String, ConcurrentHashMap<String, Integer>> categories) {
        try {
            BatchWriter writer = getConnector().createBatchWriter(SUMMARY_TABLE, new BatchWriterConfig());
            Mutation summary = new Mutation(row);
            // add a row with summary info of languages, topics
            Gson objGson = new Gson();
            Iterator it = categories.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                Text category = new Text((String) pair.getKey());
                String categoryMapStr = objGson.toJson((ConcurrentHashMap<String, Integer>) pair.getValue());
                summary.put(CATEGORY_CF, category, new Value(categoryMapStr.getBytes()));
            }
            writer.addMutation(summary);
            writer.close();
        } catch (TableNotFoundException e) {
            App.logException(e);
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
    }

    private static final String LANGUAGES = "languages";
    private static final String TOPICS = "topics";

    // update for the version
    // update for the aggregate
    private static void updateCategoryTotals(){
        try {
            Gson gson = new Gson();

            Type mapType = new TypeToken<ConcurrentHashMap<String, Integer>>(){}.getType();
            ConcurrentHashMap<String, Integer> aggregateLanguages = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Integer> aggregateTopics = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Integer> aggregateProjectMetadataKeys = new ConcurrentHashMap<>();

            // sum up all of the categories
            Scanner scanner = getScanner(SUMMARY_TABLE);
            scanner.fetchColumnFamily(CATEGORY_CF);
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()) {
                Map.Entry<Key, Value> e = it.next();
                ConcurrentHashMap<String, Integer> catMap = gson.fromJson(new String(e.getValue().get()), mapType);

                if (e.getKey().getColumnQualifier().toString().equalsIgnoreCase(LANGUAGES)) {
                    catMap.forEach((k,v) -> aggregateLanguages.merge(k,v, Integer::sum));
                } else if (e.getKey().getColumnQualifier().toString().equalsIgnoreCase(TOPICS)) {
                    catMap.forEach((k,v) -> aggregateTopics.merge(k,v, Integer::sum));
                } else if (e.getKey().getColumnQualifier().toString().equalsIgnoreCase(PROJECT_METADATA_KEYS)) {
                    catMap.forEach((k,v) -> aggregateProjectMetadataKeys.merge(k,v, Integer::sum));
                }
            }
            BatchWriter writer = getConnector().createBatchWriter(SUMMARY_TABLE, new BatchWriterConfig());
            Mutation totals = new Mutation(TOTALS);
            totals.put(CATEGORY_CF, LANGUAGES_CQ, new Value(gson.toJson(aggregateLanguages).getBytes()));
            totals.put(CATEGORY_CF, TOPICS_CQ, new Value(gson.toJson(aggregateTopics).getBytes()));
            totals.put(CATEGORY_CF, PROJECT_METADATA_KEYS_CQ, new Value(gson.toJson(aggregateProjectMetadataKeys).getBytes()));

            writer.addMutation(totals);
            writer.close();
        } catch (Exception e) {
            App.logException(e);
        }
    }

    private static void updateSingleStatsTotals(){
        // project count - sum up all project counts
        try {
            int projectCount =0;
            Scanner scanner = getScanner(SUMMARY_TABLE);
            scanner.fetchColumn(SUMMARY_STATS_CF, PROJECT_COUNT_CQ);
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()){
                Map.Entry<Key, Value> e = it.next();
                projectCount += Integer.parseInt(new String(e.getValue().get()));
            }
            BatchWriter writer = getConnector().createBatchWriter(SUMMARY_TABLE, new BatchWriterConfig());
            Mutation totals = new Mutation(TOTALS);
            totals.put(SUMMARY_STATS_CF, PROJECT_COUNT_CQ, new Value(Long.toString(projectCount).getBytes()));

            Gson gson = new Gson();
            Type hashSetType = new TypeToken<HashSet<String>>(){}.getType();

            HashSet<String> autoSuggestValues = new HashSet<>();
            scanner.fetchColumn(SUMMARY_HELPER_CF, CORPUS_AUTOSUGGEST_VALUES_CQ);
            // go ahead and reset the iterator;
            it = scanner.iterator();
            if (it.hasNext()){
                Map.Entry<Key, Value> e = it.next();
                autoSuggestValues.addAll(gson.fromJson(new String(e.getValue().get()),hashSetType));
            }

            totals.put(SUMMARY_HELPER_CF, CORPUS_AUTOSUGGEST_VALUES_CQ,
                    new Value(gson.toJson(autoSuggestValues).getBytes()));
            writer.addMutation(totals);
            writer.close();
        } catch (Exception e) {
            App.logException(e);
        }
    }

    private static void updateMsdStatsTotals(){
        try {

/* streaming combos and pools
            Scanner scanner = getScanner(SUMMARY_TABLE);
            scanner.fetchColumnFamily(SUMMARY_STATS_CF);
            Type metadataValueStatsType = new TypeToken<MetadataValueStats>(){}.getType();

            MetadataValueStats projectSizes = new MetadataValueStats();
            MetadataValueStats watchers = new MetadataValueStats();
            MetadataValueStats stargazers = new MetadataValueStats();
            MetadataValueStats forks = new MetadataValueStats();

            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while(it.hasNext()){
                Map.Entry<Key, Value> e = it.next();
                String cq = e.getApiKey().getColumnQualifier().toString();
                if (cq.equalsIgnoreCase(PROJECT_COUNT)){
                    Integer.parseInt(new String(e.getValue().get()));
                }else {
                    MetadataValueStats stats = gson.fromJson(new String (e.getValue().get()), metadataValueStatsType);
                    if (cq.equalsIgnoreCase(PROJECT_WATCHERS_STATS)){
                        watchers.recalc(stats);
                    } else if (e.getApiKey().getColumnQualifier()
                            .toString().equalsIgnoreCase(PROJECT_FORKS_STATS)){
                        forks.recalc(stats);
                    } else if (e.getApiKey().getColumnQualifier()
                            .toString().equalsIgnoreCase(PROJECT_STARGAZERS_STATS)){
                        stargazers.recalc(stats);
                    }else if (e.getApiKey().getColumnQualifier()
                            .toString().equalsIgnoreCase(PROJECT_SIZE_STATS)){
                        projectSizes.recalc(stats);
                    }
                }
            }
*/

            // temporary!!!! hard-coded versions
            HashSet<String> versions = new HashSet<>();
            Scanner scanner = getScanner(SUMMARY_TABLE);
            scanner.fetchColumnFamily(SUMMARY_STATS_CF);
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()){
                Map.Entry<Key, Value> e = it.next();
                String row = e.getKey().getRow().toString();
                if (row.startsWith("5.")){  // change this
                    versions.add(row);
                }
            }

            writeStatsToSummaryTable(TOTALS, versions);
        } catch (Exception e) {
            App.logException(e);
        }
    }

    private static void writeStatsToSummaryTable(String row, HashSet<String> versions){
        try{
            Gson gson= new Gson();

            // set up the futures that will simultaneously collect some stats
            BatchWriter writer = getConnector().createBatchWriter(SUMMARY_TABLE, new BatchWriterConfig());
            Mutation mutation = new Mutation(row);

            ExecutorService executorService = Executors.newFixedThreadPool(4);

            Future<MetadataValueStats> stargazersFuture = executorService.submit(getCallable(versions,PROJECT_STARGAZERS_STATS, "stargazers_count", 1.0));
            Future<MetadataValueStats> watchersFuture = executorService.submit(getCallable(versions,PROJECT_WATCHERS_STATS, "watchers", 1.0));
            Future<MetadataValueStats> forksFuture = executorService.submit(getCallable(versions,PROJECT_FORKS_STATS, "forks", 1.0));
            Future<MetadataValueStats> projectSizeFuture = executorService.submit(getCallable(versions,PROJECT_SIZE_STATS + " (MB)", "project_size", 1024 * 1024));

            // these calls block until each one completes
            mutation.put(SUMMARY_STATS_CF, PROJECT_STARGAZERS_STATS_CQ, new Value(gson.toJson(stargazersFuture.get()).getBytes()));
            mutation.put(SUMMARY_STATS_CF, PROJECT_WATCHERS_STATS_CQ, new Value(gson.toJson(watchersFuture.get()).getBytes()));
            mutation.put(SUMMARY_STATS_CF, PROJECT_FORKS_STATS_CQ, new Value(gson.toJson(forksFuture.get()).getBytes()));
            mutation.put(SUMMARY_STATS_CF, PROJECT_SIZE_STATS_CQ, new Value(gson.toJson(projectSizeFuture.get()).getBytes()));

            writer.addMutation(mutation);
            writer.close();

        }catch(Exception e){
            App.logException(e);
        }
    }

    private static Callable<MetadataValueStats> getCallable(HashSet<String> versions, String name, String metadatakey, double factor){
        return () -> {
            return new MetadataValueStats(versions, name, metadatakey, factor);
        };
    }
    private static void clearTotals(){
        try {
            BatchDeleter bd = getConnector().createBatchDeleter(SUMMARY_TABLE, new Authorizations(), 2, new BatchWriterConfig());
            bd.setRanges(Collections.singleton(Range.exact(TOTALS_ROW)));
            bd.delete();
            bd.close();
        } catch (Exception e) {
            App.logException(e);
        }
    }
    public static void updateSummaryTableTotals() {
        try {
            clearTotals();
            updateCategoryTotals();
            updateMsdStatsTotals();
            updateSingleStatsTotals();
        }catch(Exception e){
            App.logException(e);
        }
    }

    public static void updateSummaryTableByVersion(String version, HashMap<String, ConcurrentHashMap<String, Integer>> categoryMap) {
        try{
            writeCategoriesToSummaryTable(version, categoryMap);
            Gson gson = new Gson();

            // topics and languages each come in as one big string
            ConcurrentHashMap<String, Integer> currentLanguages = categoryMap.get(LANGUAGES);
            ConcurrentHashMap<String, Integer> currentTopics = categoryMap.get(TOPICS);
            ConcurrentHashMap<String, Integer> currentProjectMetadataKeys = categoryMap.get(PROJECT_METADATA_KEYS);

            HashSet<String> corpusAutosuggestValues = new HashSet<>();
            corpusAutosuggestValues.addAll(currentLanguages.keySet());
            corpusAutosuggestValues.addAll(currentTopics.keySet());
            corpusAutosuggestValues.addAll(currentProjectMetadataKeys.keySet());
            String corpusAutosuggestValuesJson = gson.toJson(corpusAutosuggestValues);

            BatchWriter writer = getConnector().createBatchWriter(SUMMARY_TABLE, new BatchWriterConfig());

            // update the corpus version
            Mutation versionMutation = new Mutation(CORPUS_VERSION_ROW_TITLE);
            versionMutation.put(EMPTY_TEXT, EMPTY_TEXT, new Value(version.getBytes()));
            writer.addMutation(versionMutation);

            Mutation summary = new Mutation(version);
            int projectCount = getProjectMetadata(version, "uuid", Long.MAX_VALUE, 1L, true).size();

            summary.put(SUMMARY_HELPER_CF, CORPUS_AUTOSUGGEST_VALUES_CQ, new Value(corpusAutosuggestValuesJson.getBytes()));
            summary.put(SUMMARY_STATS_CF, PROJECT_COUNT_CQ, new Value(Long.toString(projectCount).getBytes()));

            writer.addMutation(summary);
            writer.close();

            writeStatsToSummaryTable(version, new HashSet<>(Arrays.asList(version)));

            updateSummaryTableTotals();
        }catch (Exception e){
            App.logException(e);
        }
    }

    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    public static HashSet<String> getMetadataKeys() {
        return getMetadataKeys(-1, -1); // this gets them all
    }

    public static HashSet<String> getMetadataKeys(long pageSize, long page) {
        long count = 0;

        HashSet<String> rVal = new HashSet<>();
        Iterator<Map.Entry<Key, Value>> scanResults = null;
        try {
            Scanner scan = getScanner(MUSE_CORPUS);
            scan.setRange(new Range("5", "9"));
            scan.fetchColumnFamily(PROJECT_METADATA_COLFAM);
            scanResults = scan.iterator();
            if (page > 1) {
                while (scanResults.hasNext() && ++count < (page - 1) * pageSize) {
                    scanResults.next();
                }
            }
            if (pageSize > 0) {
                while (scanResults.hasNext()) {
                    rVal.add(scanResults.next().getKey().getColumnQualifier().toString());
                    if (++count % pageSize == 0) {
                        break;
                    }
                }
            } else {
                while (scanResults.hasNext()) {
                    String singleVal = scanResults.next().getKey().getColumnQualifier().toString();
                    String[] keys = singleVal.split("\\.");
                    for (String key : keys) {
                        rVal.add(key.replaceAll("\\d", ""));
                    }
                }
            }

            scan.close();
        } catch (Exception e) {
            logger.log(Level.INFO, "Exception!", e);
        }

        return rVal;
    }

    private static Range getRange(String prefix){
        //return new Range(prefix, true, prefix + "{", true);

        char lastChar = (prefix.charAt(prefix.length() - 1));
        lastChar++;
        return new Range(prefix, true, prefix.substring(0, prefix.length() - 1) + lastChar, true);

    }

    public static HashMap<String, String> queryForCount(List<List<String>> terms, int pageSize, int page, String metadataKey, boolean cache) {
        return query(terms, pageSize, page, metadataKey, cache, true);
    }

    public static HashMap<String, String> query(List<List<String>> terms, int pageSize, int page, String metadataKey, boolean cache, boolean forCount) {
        long begin = System.currentTimeMillis();
        // terms: scans are formed from DNF --> (ABC + AB'C + DD)
        // ABC, AB'C, DD are all in separate conjunctive ArrayLists
        // Conjunctive arrayLists form intersecting iterators and
        // single terms go to a separate colF scan
        HashMap<String, String> queryResults = new HashMap<>();
        ArrayList<ArrayList<Text>> conjunctiveColFs = new ArrayList<>();
        HashMap<String, String> projects = new HashMap<>();
        for (List expr : terms) {
            if (expr.size() > 1) {
                ArrayList<Text> conjunctive = new ArrayList<>();
                conjunctiveColFs.add(conjunctive);
                for (Object andTerms : expr)
                    conjunctive.add(new Text(((String) andTerms).replaceAll("[\"]", "")));
            }else{
                HashMap<String, String> projectSet = null;
                String colF = ((String)expr.get(0)).replaceAll("[\"]","");
                if (forCount) {
                    HashSet<Text> pSet = getProjectsBasedOnColFam(colF, colF.split("=")[1], pageSize, page, true);
                    projectSet = new HashMap<>();
                    for (Text t: pSet){
                        projectSet.put(t.toString(),t.toString());
                    }
                }else{
                    projectSet = ScanWrapper.getProjectsBasedOnColFamAndMetadataKey(colF,
                            colF.split("=")[1] , pageSize, page, metadataKey, true);
                }

                queryResults.putAll(projectSet);
            }
        }

        if (conjunctiveColFs.size() > 0 && queryResults.size() < pageSize) {
            int i = 0;
            for (ArrayList<Text> intersection : conjunctiveColFs) {
                try {
                    String cacheKey = new StringBuilder(intersection.toString()).append("pagesize=")
                            .append(pageSize).append("page=").append(page).append("metadataKey=")
                            .append(metadataKey).toString();
                    long count = 0;
                    Map.Entry<Key, Value> e = null;
                    HashMap<String, String> projectSet = !cache? null :
                            (HashMap<String, String>) App.resultsCache.get(cacheKey);
                    Iterator<Map.Entry<Key, Value>> scanResults;
                    if (projectSet == null) {
                        projectSet = new HashMap<>();
                        BatchScanner bs = getBatchScanner(MUSE_CORPUS);

                        IteratorSetting ii;
                        ii = new IteratorSetting(20, "iter" + i++, IntersectingIterator.class);
                        IntersectingIterator.setColumnFamilies(ii, intersection.toArray(new Text[intersection.size()]));
                        bs.addScanIterator(ii);

                        Stream<Range> a = intersection.stream().map(t -> getRange(t.toString().split("=")[1]));
                        bs.setRanges(a.collect(Collectors.toList()));

                        scanResults = bs.iterator(); scanResults.hasNext();

                        if (page > 1) {
                            while (scanResults.hasNext() && ++count < (page - 1) * pageSize) {
                                scanResults.next();
                            }
                        }
                        if (!forCount) {
                            while (scanResults.hasNext()
                                    && projectSet.size() + queryResults.size() < pageSize) {
                                e = scanResults.next();
                                Value v = e.getValue();
                                String row = e.getKey().getColumnQualifier().toString();
                                projectSet.put(row, getProjectMetadataItem(row, metadataKey));
                                if (++count % pageSize == 0) {
                                    break;
                                }
                            }
                        }else{
                            while (scanResults.hasNext()
                                    && projectSet.size() + queryResults.size() < pageSize) {
                                e = scanResults.next();
                                Value v = e.getValue();
                                String row = e.getKey().getColumnQualifier().toString();
                                projectSet.put(row, null);
                                if (++count % pageSize == 0) {
                                    break;
                                }
                            }
                        }
                        App.resultsCache.put(cacheKey, projectSet);
                    }
                    queryResults.putAll(projectSet); // either add the cache or new results
                } catch (Exception e) {
                    App.logException(e);
                }
            }
        }

        App.logger.info(new StringBuilder("Parsed Query(").append(terms.toString())
                .append("):").append(System.currentTimeMillis() - begin).append(" msec").toString());
        return queryResults;
    }

    public static long getProjectCount() {
        long rVal = 0;
        Scanner scanner = getScanner(SUMMARY_TABLE);
        scanner.fetchColumn(SUMMARY_STATS_CF, PROJECT_COUNT_CQ);
        scanner.setRange(Range.exact(TOTALS_ROW));
        Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
        while (it.hasNext()){
            Map.Entry<Key, Value> e = it.next();
            rVal += Long.parseLong(new String(e.getValue().get()));
        }
        return rVal;
    }

    // updates
    // if the caller is a write-peer, copy just the value
    // if the caller is the owner update the record with
    // the non-null fields.
    public static void updateUserDataRec(String apiKey, String projectRow, Text colFam, UserDataRecord callerUserDataRecord) throws Exception {
        if (Security.isMuseUser(apiKey)) {
            String alias = Security.getAlias(apiKey);
            String owner = callerUserDataRecord.getOwner();
            if (owner == null){
                callerUserDataRecord.setOwner(alias);
            }

            String targetColq = getFullKeyName(callerUserDataRecord.getKey(), owner);

            // can only update an existing record
            UserDataRecord userDataRecordToUpdate= getUserDataRecord(MUSE_USER_DATA_TABLE, projectRow, colFam, targetColq);
            if (userDataRecordToUpdate != null) {
                // if you are allowed to replace what's there, go for it
                // if the caller doesn't own the record, it's just a value change
                // if the caller owns the record, it's a replace
                if (Security.isAuthorizedForWrite(apiKey, callerUserDataRecord)) {
                    // check if this is an update or replace
                    BatchWriter writer = getConnector().createBatchWriter(MUSE_USER_DATA_TABLE, new BatchWriterConfig());
                    Mutation data = new Mutation(projectRow);

                    Gson objGson = new Gson();
                    // if caller is owner copy the whole record!
                    if (callerUserDataRecord.getOwner().equalsIgnoreCase(alias)) {
                        userDataRecordToUpdate.update(callerUserDataRecord);
                        data.put(colFam, new Text(targetColq),
                                new Value(objGson.toJson(userDataRecordToUpdate).getBytes()));
                    } else { // not owner so just update the value
                        userDataRecordToUpdate.setValue(callerUserDataRecord.getValue());
                        data.put(colFam, new Text(getFullKeyName(userDataRecordToUpdate.getKey(), alias)),
                                new Value(objGson.toJson(userDataRecordToUpdate).getBytes()));
                    }

                    writer.addMutation(data);
                    writer.close();
                } else {
                    throw new Exception(ExceptionBase.UNAUTHORIZED_FOR_USERDATA_WRITE);
                }
            } else {
                throw new RuntimeException(ExceptionBase.RESOURCE_NOT_FOUND); // resource already exists
            }
        }else{
            throw new RuntimeException(ExceptionBase.UNAUTHORIZED_MUSE_USERS_ONLY);
        }
    }

    // expects a key/value pair
    // ignores the 'owner' setting and sets it to the apikey's alias
    public static void postUserDataRec(String apiKey, String projectRow, Text colFam, UserDataRecord callerUserDataRecord) throws Exception {
        String alias = Security.getAlias(apiKey);
        String owner = callerUserDataRecord.getOwner();
        if (owner == null){
            callerUserDataRecord.setOwner(alias);
        }

        if (Security.isMuseUser(apiKey) && alias.equalsIgnoreCase(owner)) {
            Text targetColq = new Text (getFullKeyName(callerUserDataRecord.getKey(), owner));

            if (!recCheck(MUSE_USER_DATA_TABLE, projectRow, colFam, targetColq)) { // new record
                Gson objGson = new Gson();
                BatchWriter writer = getConnector().createBatchWriter(MUSE_USER_DATA_TABLE, new BatchWriterConfig());
                Mutation data = new Mutation(projectRow);
                callerUserDataRecord.setOwner(alias);
                data.put(colFam, new Text(targetColq), new Value(objGson.toJson(callerUserDataRecord).getBytes()));
                writer.addMutation(data);
                writer.close();
            } else {
                throw new RuntimeException(ExceptionBase.RESOURCE_CONFLICT); // resource already exists
            }
        }else{
            throw new RuntimeException(ExceptionBase.UNAUTHORIZED_MUSE_USERS_ONLY);
        }
    }

    private static Text ALIAS_CQ= new Text("alias");
    private static Text READ_PEERS_CQ = new Text("readPeers");
    private static Text WRITE_PEERS_CQ = new Text("writePeers");

    private static Text ADMIN_CF = new Text("admin");
    private static Text USER_CF = new Text("user");
    private static Text CREATED_CF = new Text("created");
    private static Text EXPIRES_CF = new Text("expires");

    public static User updateUser(User userWithUpdates){
        // used to populate the user returned from the db

        User user = getAccount(userWithUpdates.getAlias());
        user.setReadPeers(userWithUpdates.getReadPeers());
        user.setWritePeers(userWithUpdates.getWritePeers());

        createOrUpdateUser(user.getApiKey(), user);
        return user;
    }

    public static void createOrUpdateUser(String key, User user){
        try {
            BatchWriter writer = getConnector().createBatchWriter(MUSE_USERS_TABLE, new BatchWriterConfig());

            Mutation keyMutation = new Mutation(user.getApiKey());
            Text roleCf = user.getRole().equalsIgnoreCase("admin")?
                    ADMIN_CF: USER_CF;

            Gson gson = new Gson();
            // 0th row: <key> <role> expiration <date>
            Type dateType = new TypeToken<YearMonth>() {}.getType();
            keyMutation.put(roleCf, CREATED_CF,
                    new Value(gson.toJson(user.getCreated(), dateType).getBytes()));
            keyMutation.put(roleCf, EXPIRES_CF,
                    new Value(gson.toJson(user.getExpires(), dateType).getBytes()));

            // 1st row: <key> <role> alias <alias>
            keyMutation.put(roleCf, ALIAS_CQ, new Value(user.getAlias().getBytes()));

            Type listType = new TypeToken<List<String>>() {}.getType();
            List<String> readPeers = user.getReadPeers();
            if (readPeers != null) {
                // 2nd row: <key> <role> readPeers <readPeers>
                keyMutation.put(roleCf, READ_PEERS_CQ, new Value(gson.toJson(readPeers,listType).getBytes()));
            }

            List<String> writePeers = user.getWritePeers();
            if (writePeers != null) {
                // 3rd row: <key> <role> writePeers <writePeers>
                keyMutation.put(roleCf, WRITE_PEERS_CQ, new Value(gson.toJson(writePeers, listType).getBytes()));
            }
            writer.addMutation(keyMutation);

            // Partition Index
            // 4th row: <role> alias=<alias> <key> <>
            Mutation roleMutation = new Mutation(roleCf);
            Text aliasAsCf = new Text(new StringBuilder("alias=").append(user.getAlias()).toString());
            roleMutation.put(aliasAsCf,new Text(key),new Value(""));
            writer.addMutation(roleMutation);

            writer.close();
        } catch (TableNotFoundException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.TABLE_NOT_FOUND);
        } catch (MutationsRejectedException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.MUTATION_REJECTED);
        }
    }

    public static User newUser(User user) {
        String apiKey = user.getApiKey();
        if (user.getApiKey() == null) {
            // generate and set the key
            SecureRandom keyGen = new SecureRandom();
            byte bytes[] = new byte[256];
            keyGen.nextBytes(bytes);
            apiKey = Base64.getEncoder().encodeToString(bytes);
        }else{
            Security.testForValidKey(apiKey);
        }

        user.setApiKey(apiKey);
        createOrUpdateUser(user.getApiKey(), user);
        return user;
    }

    // check if a record exists
    // primarily used for create, update, and delete
    public static boolean recCheck(String table, String rowToCheck, Text colFam, Text colQ){
        Text row = new Text(rowToCheck);
        Scanner scanner = getScanner(table);
        scanner.setRange(Range.exact(row));
        scanner.fetchColumn(colFam,colQ);
        Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
        return it.hasNext();
    }

    // user data table key format is bound to the alias
    // <keyName>::(<alias>)
    private static String getFullKeyName(String key, String alias){
        return new StringBuilder(key).append("::(").append(alias).append(")").toString();
    }

    // Deletes a row from the user data table
    public static void deleteUserDataRow(String apiKey, String row, Text colFam, String keyToDelete) throws Exception {
        String alias = Security.getAlias(apiKey);
        if (Security.isMuseUser(apiKey)){
            Text colQ = new Text(getFullKeyName(keyToDelete, alias));

            if (recCheck(MUSE_USER_DATA_TABLE, row, colFam, colQ)) {
                deleteRecord(MUSE_USER_DATA_TABLE, row, colFam, colQ);
            }else{
                throw new RuntimeException(ExceptionBase.RESOURCE_NOT_FOUND);
            }
        }else{
            throw new RuntimeException(ExceptionBase.UNAUTHORIZED_MUSE_USERS_ONLY);
        }
    }

    public static void deleteRecord(String table, String row, Text colF, Text colQ){
        try {
            Mutation deleteMutation = new Mutation(row);
            deleteMutation.putDelete(colF, new Text(colQ));
            BatchWriter bw = getConnector().createBatchWriter(table, new BatchWriterConfig());
            bw.addMutation(deleteMutation);
            bw.flush();
            bw.close();
        } catch (TableNotFoundException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.TABLE_NOT_FOUND);
        } catch (MutationsRejectedException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.MUTATION_REJECTED);
        }
    }

    public static List<User> getAllAccounts() {
        ArrayList<User> accounts = new ArrayList<>();

        Text roles[] = {ADMIN_CF, USER_CF};
        Scanner scanner = getScanner(MUSE_USERS_TABLE);

        for (Text role : roles){
            scanner.fetchColumn(role, ALIAS_CQ);
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()) {
                Map.Entry<Key, Value> next = it.next();
                String alias = next.getValue().toString();
                accounts.add(getAccount(alias));
            }
        }
        return accounts;
    }


    public static User getAccount(String alias){
        /*
             <key>          <admin|user>:alias
             <key>          <admin|user>:readPeers
             <key>          <admin|user>:writePeers
             <admin|user>   alias=<alias>:<key>
        */
        Gson gson = new Gson();
        Type listType = new TypeToken<List<String>>(){}.getType();
        List<String> readPeers = null;
        List<String> writePeers = null;

        Type dateType = new TypeToken<YearMonth>(){}.getType();
        YearMonth created = null;
        YearMonth expires = null;

        String apiKey = Security.getApiKey(alias);
        String role = Security.getRole(alias, apiKey);

        Scanner scanner = getScanner(MUSE_USERS_TABLE);
        scanner.setRange(Range.exact(apiKey));
        scanner.fetchColumnFamily(new Text(role));
        Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
        while (it.hasNext()){
            Map.Entry<Key, Value> next = it.next();
            Key key = next.getKey();
            String value = new String(next.getValue().get());
            String colQ = key.getColumnQualifier().toString();
            if (colQ.equalsIgnoreCase("readPeers")){
                readPeers = gson.fromJson(value, listType);
            }else if (colQ.equalsIgnoreCase("writePeers")){
                writePeers = gson.fromJson(value, listType);
            }else if (colQ.equalsIgnoreCase("created")){
                created = gson.fromJson(value, dateType);
            }else if (colQ.equalsIgnoreCase("expires")){
                expires = gson.fromJson(value, dateType);
            }
        }
        User user = new User();
        user.setAlias(alias);
        user.setApiKey(apiKey);
        user.setRole(role);
        user.setReadPeers(readPeers);
        user.setWritePeers(writePeers);
        user.setCreated(created);
        user.setExpires(expires);

        return user;
    }

    /* Only an admin can delete a user
       -- cleans up the user table
              Row               CF:CQ                    VALUE
             <key>          <admin|user>:alias          <alias>
             <key>          <admin|user>:readPeers      <readPeer list>
             <key>          <admin|user>:writePeers     <writePeer list>
             <admin|user>   alias=<alias>:<key>             []

       -- cleans up the user data table
        ROW   <RowId>
        CF:CQ projectMetadata:<metadata key>::(<alias>)
        CF:CQ projectFileMetadata:<metadata key>|FileHash::(<alias>)
        value {"owner":<alias>,
                "key":<key>,
                "value":<value>,
                "elapsedTimeMsec":0}

       -- cleans up readPeers and writePeers mentioned
            mentioned in accounts
            mentioned in records
    */
    public static void deleteUser(User user) {
        String aliasToDelete = user.getAlias();
        String apiKeyToDelete = user.getApiKey();
        Text roleToDelete = new Text(user.getRole());

        // Part 0: Disable the account
        Security.disableAccount(apiKeyToDelete);

        // Part 1: Delete the user from the user table
            /*
                delete rows:
             <key>          <admin|user>:alias
             <key>          <admin|user>:readPeers
             <key>          <admin|user>:writePeers
             <key>          <admin|user>:created
             <key>          <admin|user>:expires
             <admin|user>   alias=<alias>:<key>
            */
        deleteRecordWithCheck(MUSE_USERS_TABLE, apiKeyToDelete, roleToDelete, ALIAS_CQ);
        deleteRecordWithCheck(MUSE_USERS_TABLE, apiKeyToDelete, roleToDelete, READ_PEERS_CQ);
        deleteRecordWithCheck(MUSE_USERS_TABLE, apiKeyToDelete, roleToDelete, WRITE_PEERS_CQ);
        deleteRecordWithCheck(MUSE_USERS_TABLE, apiKeyToDelete, roleToDelete, CREATED_CF);
        deleteRecordWithCheck(MUSE_USERS_TABLE, apiKeyToDelete, roleToDelete, EXPIRES_CF);

        ///////////////////////////////////////////////
        // saving this delete until the end basically blocks
        // any new account creation with this alias until
        // this account deletion is complete
        // Text aliasCqToDeleteCq = new Text(new StringBuilder("alias=").append(aliasToDelete).toString());
        // deleteRecordWithCheck(MUSE_USERS_TABLE, roleToDelete.toString(), aliasCqToDeleteCq, new Text(apiKeyToDelete));
        ///////////////////////////////////////////////

        // Part 2: Delete the user's entries into the user_data_table
        //specifying which iterator class you are using
        deleteRecordsBasedOnAlias(aliasToDelete);

        /////////////////////////////////////////////////////////////
            /*  Part 3:
                   -- cleans up readPeers and writePeers
                        mentioned in accounts (shouldn't take long)
                 RowId              CF              CQ
                <key>          admin|user:readPeers|writePeers
            */
        removeAliasFromAllUserPeers(aliasToDelete, ADMIN_CF, READ_PEERS_CQ);
        removeAliasFromAllUserPeers(aliasToDelete, ADMIN_CF, WRITE_PEERS_CQ);
        removeAliasFromAllUserPeers(aliasToDelete, USER_CF, READ_PEERS_CQ);
        removeAliasFromAllUserPeers(aliasToDelete, USER_CF, WRITE_PEERS_CQ);
        ////////////////////////////////////////////////////////////

        /*  Part 4:
               -- cleans up readPeers and writePeers
                  mentioned in records  (will take very long if scanning the whole table)
        */
        removeAliasFromAllUserDataRecords(aliasToDelete);

        // delete the "disabled" record
        deleteRecordWithCheck(MUSE_USERS_TABLE, apiKeyToDelete, DISABLED_CQ, new Text(""));


        // saving this delete until the end; basically blocks
        // any new account creation with this alias until
        // this account deletion is complete
        Text aliasCqToDeleteCq = new Text(new StringBuilder("alias=").append(aliasToDelete).toString());
        deleteRecordWithCheck(MUSE_USERS_TABLE, roleToDelete.toString(), aliasCqToDeleteCq, new Text(apiKeyToDelete));
        ///////////////////////////////////////////////
    }

    private static final Text DISABLED_CQ = new Text("disabled");
    private static final IteratorSetting PROJECT_METADATA_ITERATOR_SETTING = new IteratorSetting(20, "userProjectMetadataFilter", RegExFilter.class);
    private static final IteratorSetting PROJECT_FILES_METADATA_ITERATOR_SETTING = new IteratorSetting(21, "userProjectFilesMetadataFilter", RegExFilter.class);

    private static Iterator<Map.Entry<Key, Value>> getIteratorBasedOnRegexFilterAndColFam(
            String rowRegEx,
            String colqRegEx,
            String valRegEx,
            String type){

        boolean orFields = false;

        //now add the iterator to the scanner, and you're all set
        BatchScanner bs = getBatchScanner(MUSE_USER_DATA_TABLE);

        if (type.equalsIgnoreCase(PROJECT_METADATA)
                || type.equalsIgnoreCase(ALL_METADATA)){
            RegExFilter.setRegexs(PROJECT_METADATA_ITERATOR_SETTING, null, PROJECT_METADATA, colqRegEx, valRegEx, orFields);
            bs.removeScanIterator("userProjectMetadataFilter");
            bs.addScanIterator(PROJECT_METADATA_ITERATOR_SETTING);
        }
        if (type.equalsIgnoreCase(PROJECT_FILES_METADATA)
                || type.equalsIgnoreCase(ALL_METADATA)){
            RegExFilter.setRegexs(PROJECT_FILES_METADATA_ITERATOR_SETTING, null, PROJECT_FILES_METADATA, colqRegEx, valRegEx, orFields);
            bs.removeScanIterator("userProjectFilesMetadataFilter");
            bs.addScanIterator(PROJECT_FILES_METADATA_ITERATOR_SETTING);
        }

        bs.setRanges(Collections.singleton(Range.prefix("5.0")));
        return bs.iterator();
    }



    // users regex filter to delete
    private static void deleteRecordsBasedOnAlias(String aliasToDelete){
        ArrayList<Mutation> deleteMutations = new ArrayList<>();
        //next set the regular expressions to match. Here, I want all key/value pairs in
        //which the column family begins with "*::(="
        //         ROW   <RowId>
        //        CF:CQ projectMetadata|contentMetadata:<metadata key::(<alias>)>

        String colq = new StringBuilder(".+::(").append(aliasToDelete).append(")").toString();
        Iterator<Map.Entry<Key, Value>> recsToDelete = getIteratorBasedOnRegexFilterAndColFam(null, colq, null, ALL_METADATA);
        while (recsToDelete.hasNext()){
            Map.Entry<Key, Value> next = recsToDelete.next();
            Text row = next.getKey().getRow();
            Text cf = next.getKey().getColumnFamily();
            Text cq = next.getKey().getColumnQualifier();

            Mutation deleteMutation = new Mutation(row);
            deleteMutation.putDelete(cf, cq);

            deleteMutations.add(deleteMutation);
        }
        deleteRecords(MUSE_USER_DATA_TABLE, deleteMutations);
    }

    private static void removeAliasFromAllUserDataRecords(String aliasToDelete) {
        try {
            Gson gson = new Gson();
            Type userDataRecordType = new TypeToken<UserDataRecord>(){}.getType();

            Scanner scanner = getScanner(MUSE_USER_DATA_TABLE);
            BatchWriter writer = getConnector().createBatchWriter(MUSE_USER_DATA_TABLE, new BatchWriterConfig());
            UserDataRecord userDataRecord = null;

            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()){
                Map.Entry<Key, Value> e = it.next();
                Key key = e.getKey();
                Mutation updateMutation = new Mutation(key.getRow());
                userDataRecord = gson.fromJson(new String(e.getValue().get()), userDataRecordType);

                // remove the alias and insert the updated List<String>
                List<String> readPeers = userDataRecord.getReadPeers();
                List<String> writePeers = userDataRecord.getWritePeers();

                if (null != readPeers) {
                    readPeers.removeIf(alias -> alias.equalsIgnoreCase(aliasToDelete));
                    userDataRecord.setReadPeers(readPeers);
                }

                if (null != writePeers) {
                    writePeers.removeIf(alias -> alias.equalsIgnoreCase(aliasToDelete));
                    userDataRecord.setWritePeers(writePeers);
                }

                // no point to mutating an irrelevant record
                if (null != readPeers && null != writePeers) {
                    updateMutation.put(key.getColumnFamily(), key.getColumnQualifier(), new Value(gson.toJson(userDataRecord, userDataRecordType).getBytes()));
                    writer.addMutation(updateMutation);
                }
            }
            writer.flush();
            writer.close();
        } catch (TableNotFoundException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.TABLE_NOT_FOUND);
        } catch (MutationsRejectedException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.MUTATION_REJECTED);
        }
    }

    public static void insert(String table, String row, Text colF, Text colQ, Value val){
        try {
            BatchWriter writer = getConnector().createBatchWriter(table, new BatchWriterConfig());
            Mutation mutation = new Mutation(row);
            mutation.put(colF,colQ, val);
            writer.addMutation(mutation);
            writer.flush();
            writer.close();
        } catch (TableNotFoundException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.TABLE_NOT_FOUND);
        } catch (MutationsRejectedException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.MUTATION_REJECTED);
        }
    }

    private static void removeAliasFromAllUserPeers(String aliasToDelete, Text role, Text peerType){
        try {
            Gson gson = new Gson();
            Type listType = new TypeToken<List<String>>(){}.getType();

            // readPeers and writePeers for both admin and user roles
            Scanner scanner = getScanner(MUSE_USERS_TABLE);
            BatchWriter writer = getConnector().createBatchWriter(MUSE_USERS_TABLE, new BatchWriterConfig());
            ArrayList<String> peers = null;
            scanner.fetchColumn(role, peerType);
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            while (it.hasNext()){
                Map.Entry<Key, Value> e = it.next();
                Mutation peerMutation = new Mutation(e.getKey().getRow());
                peers = gson.fromJson(new String(e.getValue().get()), listType);

                // remove the alias and insert the updated List<String>
                peers.removeIf(alias -> alias.equalsIgnoreCase(aliasToDelete));
                peerMutation.put(role, peerType, new Value(gson.toJson(peers,listType).getBytes()));
                writer.addMutation(peerMutation);
            }
            writer.flush();
            writer.close();
        } catch (TableNotFoundException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.TABLE_NOT_FOUND);
        } catch (MutationsRejectedException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.MUTATION_REJECTED);
        }
    }
    private static void deleteRecordWithCheck(String table, String row, Text cf, Text cq){
        if (recCheck(table, row, cf, cq)) {
            try {
                Mutation deleteMutation = new Mutation(row);
                deleteMutation.putDelete(cf, cq);
                BatchWriter bw = getConnector().createBatchWriter(table, new BatchWriterConfig());
                bw.addMutation(deleteMutation);
                bw.flush();
                bw.close();
            } catch (TableNotFoundException e) {
                App.logException(e);
                throw new RuntimeException(ExceptionBase.TABLE_NOT_FOUND);
            } catch (MutationsRejectedException e) {
                App.logException(e);
                throw new RuntimeException(ExceptionBase.MUTATION_REJECTED);
            }
        }
    }

    private static void deleteRecords(String table, ArrayList<Mutation> mutations){
        try {
            BatchWriter bw = getConnector().createBatchWriter(table, new BatchWriterConfig());
            bw.addMutations(mutations);
            bw.flush();
            bw.close();
        } catch (TableNotFoundException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.TABLE_NOT_FOUND);
        } catch (MutationsRejectedException e) {
            App.logException(e);
            throw new RuntimeException(ExceptionBase.MUTATION_REJECTED);
        }
    }

    public static boolean isAliasUnique(String alias) {
        boolean isUnique = false;
        Scanner scanner = getScanner(MUSE_USERS_TABLE);
        Text specificAlias = new Text(new StringBuilder("alias=").append(alias).toString());
        scanner.fetchColumnFamily(specificAlias);
        if (!scanner.iterator().hasNext()){
            isUnique = true;
        }
        return isUnique;
    }
}
        /* Scratch -
        //specifying which iterator class you are using
        IteratorSetting iter = new IteratorSetting(15, "myFilter", RegExFilter.class);
//next set the regular expressions to match. Here, I want all key/value pairs in
//which the column family begins with "language="
        String rowRegex = null;
        String colfRegex = "language="+colFam;
        String colqRegex = null;
        String valueRegex = null;
        boolean orFields = false;
        RegExFilter.setRegexs(iter, rowRegex, colfRegex, colqRegex, valueRegex, orFields);
//now add the iterator to the scanner, and you're all set
        //scan.removeScanIterator("myFilter");
        //scan.addScanIterator(iter);
        */

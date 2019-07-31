package com.twosixlabs.muse_utils;

import com.twosixlabs.model.accumulo.ScanWrapper;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;

import static com.twosixlabs.model.accumulo.ScanWrapper.MUSE_USERS_TABLE;
import static com.twosixlabs.model.accumulo.ScanWrapper.MUSE_USER_DATA_TABLE;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.exit;

public class Ingest {

    private static final JSONParser parser = new JSONParser();
    private static final int NUM_OF_FILES_OF_INTEREST = 4;


    // batch >= retrieval
    // batch % retrieval = 0
    private static final int RETRIEVAL_SIZE = 5000;
    private static final int INJECT_BATCH_SIZE = 10000;

    private static HashSet<File> dirs = new HashSet<>();
    private static final HashSet<File> files = new HashSet<>();
    private static final Logger logger = Logger.getLogger(Ingest.class.getName());
    private static FileHandler fh;
    private static final Value DUMMY_VAL = new Value("");
    private static String location;
    private static String user;
    private static String pass;
    private static String name;
    private static String currentVersion;
    private static String port;
    private static String sourceDir;
    private static String targetDir;
    private static boolean injectInstrumentation;

    private static void clearFilesAndDirs(){
        try {
            for (File f: files){
                f.delete();
            }
        } catch (Exception e) {
            logger.log(Level.INFO, "Exception while trying to delete + f: " + e.getMessage(), e);
        }
        files.clear();
        // remove dirs from untarred
        for (File d : dirs) {
            try {
                //Deleting the directory recursively using FileUtils.
                FileUtils.deleteDirectory(d);
            } catch (IOException e) {
                logger.log(Level.INFO, "Exception while trying to delete: " + d + e.getMessage(), e);
            }
        }
        dirs.clear();
    }

    private static void decompress(String in) {
        try (TarArchiveInputStream fin = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(in)))){
            TarArchiveEntry entry;
            int index = in.lastIndexOf("_meta");
            String projectKey = in.substring(in.substring(0, index).lastIndexOf("/") + 1, index);

            while ((entry = fin.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                File curfile = new File(targetDir, entry.getName());
                File parent = curfile.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                    dirs.add(parent);
                }
                FileOutputStream curFileStrm = new FileOutputStream(curfile);
                IOUtils.copy(fin, curFileStrm);
                curFileStrm.close();
                files.add(curfile);
            }
            dirs.add(new File("./"+ projectKey + "_metadata.tgz.dir"));
            if (files.stream().mapToInt(f->f.getName().contains("index")? 1 :0).sum() > 0) {
                collect(files, projectKey);
            }else{// malformed probably missing parent dir in the list
                logger.info("Malformed tgz: " + in);
            }
            clearFilesAndDirs();
        }catch (Exception e){
            logger.log(Level.INFO, "decompress: " + e.getMessage() + " " + in, e);
        }
    }

    private static void setupConditions(String version){
        try {
            location = App.getLocation();
            user = App.getUser();
            pass = App.getPass();
            name = App.getName();

            currentVersion = version;
            App.setCurrentVersion(currentVersion);

            port = App.getPort();
            ScanWrapper.initialize(currentVersion, port);
            sourceDir = App.getSourceDir();
            targetDir = App.getTargetDir();
            injectInstrumentation = App.isInjectInstrumentation();
        } catch (Exception e) {
            logException(e);
            exit(1);
        }
    }

    // things we want updated in the spawned threads
    private final static String TOPIC_CATEGORY = "topics";
    private final static String LANGUAGE_CATEGORY = "languages";
    private final static String PROJECT_METADATA_KEYS = "projectMetadataKeys";

    private static final ConcurrentHashMap<String, Integer> LANGUAGE_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Integer> TOPIC_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Integer> PROJECTMETADATAKEYS_MAP = new ConcurrentHashMap<>();

    private static final HashMap<String, ConcurrentHashMap<String, Integer>> CATEGORY_MAP = new HashMap<>();

    private static void initializeCategoryMap(){
        CATEGORY_MAP.put(TOPIC_CATEGORY, TOPIC_MAP);
        CATEGORY_MAP.put(LANGUAGE_CATEGORY, LANGUAGE_MAP);
        CATEGORY_MAP.put(PROJECT_METADATA_KEYS, PROJECTMETADATAKEYS_MAP);
    }

    public static void main(String version){
        initializeCategoryMap();

        int counter=0;
        Iterator<Path> miniTgz;
        String currentFile = null;
        ArrayList<Thread> threadList = new ArrayList<>();

        try {
            // logger and arg parsing setup
            logger.setUseParentHandlers(false);
            fh = new FileHandler("./ingest.log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            setupConditions(version);

            long start = currentTimeMillis();
            ArrayList<Long> aves = new ArrayList<>();

            miniTgz = Files.newDirectoryStream(Paths.get(sourceDir), path -> path.toString().endsWith("mini.tar.gz")).iterator();
            logger.info("File list acquired and beginning...");
            long count =0;
            while(miniTgz.hasNext()){
                long retrievalCounter = 0;
                if (!injectInstrumentation) {
                    while(miniTgz.hasNext() && retrievalCounter++ < RETRIEVAL_SIZE) {
                        currentFile = miniTgz.next().toString();
                        decompress(currentFile);
                    }
                }else{
                    while(miniTgz.hasNext() && retrievalCounter++ < RETRIEVAL_SIZE) {
                        currentFile = miniTgz.next().toString();
                        long decompressStart = currentTimeMillis();
                        decompress(currentFile);
                        long decompressEnd = currentTimeMillis();
                        aves.add(decompressEnd - decompressStart);
                    }
                    double ave = aves.stream().mapToDouble(f -> f.doubleValue()).sum() / aves.size();
                    System.out.println("Average Decompress and Compress rate (msec): " + ave);
                    logger.info("Average Decompress and Compress rate (msec): " + ave);
                    logger.info(counter + " projects ready to be ingested.");
                }
                count += retrievalCounter -1; // reset the counter
                logger.info(count + " projects set up.");

                if (count % INJECT_BATCH_SIZE == 0) {
                    HashMap<String, HashMap<String,String>> chunk = projectData;

                    // start the thread
                    Runnable ingestChunk = () -> { ingest(chunk); };
                    Thread t = new Thread(ingestChunk);
                    t.setName("Thread: " + ++counter);
                    t.start();
                    threadList.add(t);

                    logger.info(" Spawned thread to ingest " + projectData.size() + " projects.");

                    //reset the project hashmap
                    projectData = new HashMap<>();
                }
            }
            ingest(projectData); // ingest the rest

            // wait for threads to finish and
            // then update languages, topics, and keys
            // for the summary table
            for(Thread t : threadList) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    logException(e);
                }
            }
            // write out the categories to the summary table
            System.out.println("Ingest Total time (msec): " + (currentTimeMillis() - start));
            logger.log(Level.INFO,"Ingest Total time (msec): " + (currentTimeMillis() - start));

            System.out.println("Updating stats...");
            logger.log(Level.INFO,"Updating stats...");
            start = currentTimeMillis();
            // some projects have one-time project keys
            PROJECTMETADATAKEYS_MAP.entrySet().removeIf(e -> e.getValue().intValue() < 2);
            ScanWrapper.updateSummaryTableByVersion(currentVersion, CATEGORY_MAP);
            logger.log(Level.INFO,"Updating stats Total time (msec): " + (currentTimeMillis() - start));
            System.out.println("Updating stats Total time (msec): " + (currentTimeMillis() - start));
        } catch (IOException e) {
            logException(e);
        }
    }
    static private final String MUSE_CORPUS_TABLE = "muse_corpus";

    private static HashMap<String, HashMap<String,String>> projectData = new HashMap<>();
    private static HashMap<String, String> projectKeyPair = null;

    private static void collect(HashSet<File> projFiles, String projectKey) {
        try {
            projectKeyPair = new HashMap<>();

            for (File file : projFiles) {
                JSONObject jsonObj = getJsonInfo(file.getAbsolutePath());

                Set<String> keys = jsonObj.keySet();
                if (file.getName().contains("topic")) {
                    String projVal = jsonObj.keySet().toString();
                    if (!projVal.contains("None")) {
                        projectKeyPair.put("topics", projVal.toLowerCase().trim());
                    }
                } else {
                    for (String key : keys) {
                        StringBuilder projVal = new StringBuilder();
                        Object vals = jsonObj.get(key);
                        if (vals == null)
                            continue;

                        // topic keys are stored as values
                        if ((vals).getClass().isInstance(JSONArray.class)) {
                            for (Object val : (JSONArray) vals) {
                                projVal.append(val.toString());
                                projVal.append(",");
                            }
                        } else {
                            projVal.append(vals.toString());
                        }
                        projectKeyPair.put(key, vals.toString().trim());
                    }
                }
            }
            projectData.put(projectKey, projectKeyPair);
        }catch (Exception e) {
            logException(e);
        }
    }
    private static JSONObject getJsonInfo(String fName){
        JSONObject rVal = null;
        try
        {
            FileReader file = new FileReader(fName);

            Object object = parser.parse(file);

            //convert Object to JSONObject
            JSONObject jsonObject = (JSONObject)object;
            rVal = jsonObject;
            file.close();
        }catch(Exception e)
        {
            logger.info("Error with " + fName);
            logException(e);
        }
        return rVal;
    }

    private static void logException(Exception e){
        StackTraceElement[] tes = e.getStackTrace();
        StringBuilder sb = new StringBuilder();
        sb.append(e.toString()).append("\n");
        for (StackTraceElement el : tes){
            sb.append(el.toString()).append("\n");
        }
        logger.info(sb.toString());
    }
    private static Connector connector = null;

    private static Connector getConnector(){
        try {
            if (connector == null){
                String zooServers = location + ":" + port; //"localhost:44001";
                ZooKeeperInstance inst = new ZooKeeperInstance(name, zooServers);

                PasswordToken authToken = new PasswordToken(pass);

                connector = inst.getConnector(user, authToken);
            }
        } catch (AccumuloException e) {
            e.printStackTrace();
        } catch (AccumuloSecurityException e) {
            e.printStackTrace();
        }
        return connector;
    }
    private static Scanner getScanner(String table){
        Scanner scanner = null;
        try {
            Connector conn = getConnector();
            /* add a row with summary info
            languages, topics
             */
            if (!conn.tableOperations().exists(SUMMARY_TABLE))
                conn.tableOperations().create(SUMMARY_TABLE);
            scanner = conn.createScanner(table,new Authorizations());
        } catch (AccumuloException e) {
            logger.log(Level.INFO, "Exception!", e);
        } catch (AccumuloSecurityException e) {
            logger.log(Level.INFO, "Exception!", e);
        } catch (TableNotFoundException e) {
            logger.log(Level.INFO, "Exception!", e);
        } catch (TableExistsException e) {
            logger.log(Level.INFO, "Exception!", e);
        }

        return scanner;
    }
    private static HashSet<String> getSummary(String colQual){
        HashSet<String> rVal = new HashSet<>();
        try {
            Scanner scan = getScanner(SUMMARY_TABLE);
            if (scan != null){
                //scan.setRange( Range.prefix("summary"));
                scan.fetchColumn(new Text("summary"), new Text(colQual));

                Iterator<Map.Entry<Key, Value>> iter = scan.iterator();
                try {
                    while (iter.hasNext()) {
                        Map.Entry<Key, Value> v = iter.next();
                        String[] elements = v.getValue().toString()
                                .replaceAll("[\\[\\]]","").split(",");
                        for (String e: elements){
                            rVal.add(e.trim());
                        }
                    }
                } catch (Exception e) {
                    App.logException(e);
                }
                scan.clearColumns();
            }

        } catch (Exception e) {
            App.logException(e);
        }
        return rVal;
    }

    private static synchronized void addCategoryItem(String category, String item){
        ConcurrentHashMap<String, Integer> map = null;
        if (category.equalsIgnoreCase("topics")){
            map = TOPIC_MAP;
        }else if (category.equalsIgnoreCase("languages")){
            map = LANGUAGE_MAP;
        }else if (category.equalsIgnoreCase("projectMetadataKeys")){
            map = PROJECTMETADATAKEYS_MAP;
        }

        if (map != null){
            Integer count = map.get(item);
            if (count != null){
                map.put(item, ++count);
            }else{
                map.put(item, 1);
            }
        }else{
            throw new RuntimeException("Category: " + category + " not set");
        }
    }

    private static final String BOOTSTRAP_ADMIN_ACCOUNT =
            "{\"alias\":\"bootstrap- delete me after creating at least one account\",\"role\":\"admin\"}";
    private static final String BOOTSTRAP_USER_ACCOUNT =
            "{\"alias\":\"exampleUser@bogus.nada\",\"role\":\"user\"}";

    private static void ingest(HashMap<String, HashMap<String, String>> projData){
        try{
            BatchWriterConfig cfg = new BatchWriterConfig();
            //cfg.setMaxMemory(500000000);
            cfg.setMaxWriteThreads(10);


            BatchWriterOpts bwOpts = new BatchWriterOpts();
            Connector connector = getConnector();
            // let's go ahead and create our tables
            MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(bwOpts.getBatchWriterConfig());
            //MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(cfg);
            if (!connector.tableOperations().exists(MUSE_CORPUS_TABLE))
                connector.tableOperations().create(MUSE_CORPUS_TABLE);
            if (!connector.tableOperations().exists(SUMMARY_TABLE))
                connector.tableOperations().create(SUMMARY_TABLE);
            // muse_user_data
            if (!connector.tableOperations().exists(MUSE_USER_DATA_TABLE))
                connector.tableOperations().create(MUSE_USER_DATA_TABLE);
            //bootstrap muse_users
            if (!connector.tableOperations().exists(MUSE_USERS_TABLE)) {
                connector.tableOperations().create(MUSE_USERS_TABLE);
                // bootstrap with an admin account
                AccountServices.createAccount(BOOTSTRAP_ADMIN_ACCOUNT);
                AccountServices.createAccount(BOOTSTRAP_USER_ACCOUNT);
            }


            BatchWriter bw = mtbw.getBatchWriter(MUSE_CORPUS_TABLE);


            // iterate over the project map
            // and insert into meta col fam
            for (String key : projData.keySet()){
                HashMap<String, String> pData = projData.get(key);
                pData.keySet().forEach(k-> addCategoryItem(PROJECT_METADATA_KEYS, k));

                // build the rowId
                String repo = pData.get("repo");
                repo = repo == null || repo.length() == 0? "unknown_repo" : repo;

                // checking two keys for a language
                // combine them if they are both non null
                String lang = pData.get("languageMain");
                String langs = pData.get("language");
                langs = langs == null || langs.length() ==0 || langs.contains("\"") || langs.contains("null")? null : langs;

                if (lang == null || lang.length() ==0 || lang.contains("\"")||  lang.contains("null") ){
                    lang = langs == null? "unknown_main_lang" : langs;
                }else{
                    lang = langs == null? lang : lang + "," + langs;
                }

                lang = lang.toLowerCase().replaceAll("\\?certainty=[0-9]\\.[0-9]+","");

                String name = pData.get("full_name");
                name = name == null || name.contains("\"") || name.length() == 0? "unknown_full_name" : name.toLowerCase();

                String date = pData.get("created_at");
                date = date == null || date.length() == 0? "unknown_date" : date.substring(0,10); // clip to yyyy-mm-dd

                String uuid = pData.get("uuid");
                uuid = uuid == null || uuid.length() == 0? "unknown_uuid" : uuid;

                if (lang.contains(" ")){
                    logger.info(uuid + " is multi-language");
                }

                // <version|name|date|repo|uuid>
                String rowId =  new StringBuilder(currentVersion).append("|")
                        .append(name).append("|").append(date).append("|")
                        .append(repo).append("|").append(uuid).toString();

                // add the project metadata
                // add the metadatakey to the summary table
                Text colf = new Text("projectMetadata");
                Mutation m = new Mutation(rowId);
                for (Map.Entry<String,String> pair : pData.entrySet()){
                    Text metadataKey = new Text(pair.getKey());
                    m.put(colf, new Text(pair.getKey()), new Value(pair.getValue().getBytes()));
                }
                bw.addMutation(m);

                // add the document index
                // column fam is <row element= {name, date, repo, uuid}
                // this allows search based on row elements
                // Topics is stored as a string
                ArrayList<String> rowElements = new ArrayList<>(Arrays.asList(currentVersion, name, date, repo, uuid));
                List<String> colFams = new ArrayList<>(Arrays.asList("version=" + currentVersion, "name=" + name, "date=" + date, "repo=" + repo, "uuid=" + uuid));
                String topicStr = pData.get("topics");
                HashSet<String> topicColFams;
                String[] topics;
                if (topicStr != null && topicStr.length() > 0){
                    // the topic string is wrapped in brackets
                    topics = topicStr.toLowerCase().substring(1,topicStr.length()-1).split(",");

                    topicColFams = new HashSet<>(Arrays.stream(topics).map(t -> t.trim()).collect(Collectors.toList()));
                    topicColFams.forEach(t ->addCategoryItem(TOPIC_CATEGORY, t));
                    rowElements.addAll(topicColFams);
                    colFams.addAll(topicColFams.stream().map(t-> "topic=" + t).collect(Collectors.toList()));
                }

                // languages
                //added to row elements
                String[] languages = lang.split(",");
                HashSet<String> langColFams = new HashSet<>(Arrays.stream(languages).map(element -> element.trim()).collect(Collectors.toList()));
                langColFams.stream().forEach(l -> addCategoryItem(LANGUAGE_CATEGORY, l));

                rowElements.addAll(langColFams);
                colFams.addAll(langColFams.stream().map(l-> "language=" + l).collect(Collectors.toList()));

                String[] rowAr = getRowArray(rowElements);
                for (String shuffledRowId : rowAr){
                    Mutation mShuffle = new Mutation(shuffledRowId);
                    for (String colFam : colFams){
                        mShuffle.put(new Text(colFam), new Text(rowId), new Value());
                    }
                    bw.addMutation(mShuffle);
                }
            }

            mtbw.close();
            logger.info(projData.size() + " projects ingested.");
        } catch (Exception e) {
            logException(e);
        }
    }

    private static final String SUMMARY_TABLE = "muse_summary";
    private static String[] getRowArray(ArrayList<String> elements){
        return (genRowArray(elements, new ArrayList<>(), 0));
    }

    // recursive calls
    private static String[] genRowArray(ArrayList<String> elements, ArrayList<String> rowIdList, int count ){
        ArrayList<String> shuffledElements;
        shuffledElements = new ArrayList<>(elements.subList(1, elements.size()));
        shuffledElements.add(elements.get(0));
        StringBuilder sb = new StringBuilder();
        for (String e : elements){
            sb.append(e).append("|");
        }
        rowIdList.add(sb.toString().substring(0,sb.length() -1));

        if (count == elements.size() -1){
            String[] rVal = new String[rowIdList.size()];
            return(rowIdList.toArray(rVal));
        }else{
            return genRowArray(shuffledElements, rowIdList, ++count);
        }
    }
}

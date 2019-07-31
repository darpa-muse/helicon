package com.twosixlabs.muse_utils;

import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.model.entities.MetadataValueStats;
import joptsimple.AbstractOptionSpec;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.accumulo.core.client.Scanner;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.grizzly.http.server.HttpServer;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import javax.ws.rs.core.Application;

import static java.lang.System.exit;

/**
 *
 *
 */
//@ApplicationPath("muse")
public class App extends Application
{
// Basic Corpus Stats
    private static MetadataValueStats projMetaStats;
    private static CachingProvider provider;
    public static Cache<String, Object> resultsCache;

    public static MetadataValueStats getProjMetaStats() {
        return projMetaStats;
    }

    public static void setProjMetaStats(MetadataValueStats projMetaStats) {
        App.projMetaStats = projMetaStats;
    }

    public static MetadataValueStats getWatchersMetaStats() {
        return watchersMetaStats;
    }

    public static void setWatchersMetaStats(MetadataValueStats watchersMetaStats) {
        App.watchersMetaStats = watchersMetaStats;
    }

    public static MetadataValueStats getForksMetaStats() {
        return forksMetaStats;
    }

    public static void setForksMetaStats(MetadataValueStats forksMetaStats) {
        App.forksMetaStats = forksMetaStats;
    }

    private static MetadataValueStats watchersMetaStats;
    private static MetadataValueStats forksMetaStats;

    public static MetadataValueStats getStargazersStats() {
        return stargazersStats;
    }

    public static void setStargazersStats(MetadataValueStats stargazersStats) {
        App.stargazersStats = stargazersStats;
    }

    private static MetadataValueStats stargazersStats;
/////////////////////////////////////////////////////

    public static final Logger logger = Logger.getLogger(App.class.getName());
    private static FileHandler fh;
    private static Scanner scanner = null;
    public final static String ZOOKEEPER_PROD_PORT = "2181";
    public static String ACCUMULO_REST_PORT = "8052";
    public final static String ACCUMULO_REST_SERVER = "http://0.0.0.0";

    public static String getCorpusAutosuggestValues() {
        return CORPUS_AUTOSUGGEST_VALUES;
    }

    public static int getStatusCode(String msg){
        String[] eCodes = msg.split(":");
        return eCodes.length > 1? Integer.parseInt(eCodes[0]):500;
    }


    public static void logException(Exception e){
        StackTraceElement[] tes = e.getStackTrace();
        StringBuilder sb = new StringBuilder();
        sb.append(e.getMessage()).append("\n");
        for (StackTraceElement el : tes){
            sb.append(el.toString()).append("\n");
        }
        App.logger.info(sb.toString());
    }
    public static void setCorpusAutosuggestValues(String corpusAutosuggestValues) {
        CORPUS_AUTOSUGGEST_VALUES = corpusAutosuggestValues;
    }

    public static String CORPUS_AUTOSUGGEST_VALUES;
    private static final URI BASE_URI = URI.create(ACCUMULO_REST_SERVER);
    public static final String ROOT_PATH = "muse";
    public static ResourceConfig resourceConfig = new ResourceConfig().packages(true, "com.twosixlabs.resources");

    private static boolean injectInstrumentation;
    private static String currentVersion;
    private static String sourceDir;
    private static String targetDir;
    private static String user;
    private static String pass;
    private static String name;
    private static String port;

    public static String getMusePort() {
        return musePort;
    }

    public static void setMusePort(String musePort) {
        App.musePort = musePort;
    }

    public static String getBaseURI() {
        return baseURI;
    }

    public static void setBaseURI(String baseURI) {
        App.baseURI = baseURI;
    }

    private static String baseURI;
    private static String musePort;
    private static String location;

    public static boolean isInjectInstrumentation() {
        return injectInstrumentation;
    }

    public static void setInjectInstrumentation(boolean injectInstrumentation) {
        App.injectInstrumentation = injectInstrumentation;
    }

    public static String getCurrentVersion() {
        return currentVersion;
    }

    public static void setCurrentVersion(String currentVersion) {
        App.currentVersion = currentVersion;
    }

    public static String getSourceDir() {
        return sourceDir;
    }

    public static void setSourceDir(String sourceDir) {
        App.sourceDir = sourceDir;
    }

    public static String getTargetDir() {
        return targetDir;
    }

    public static void setTargetDir(String targetDir) {
        App.targetDir = targetDir;
    }

    public static String getUser() {
        return user;
    }

    public static void setUser(String user) {
        App.user = user;
    }

    public static String getPass() {
        return pass;
    }

    public static void setPass(String pass) {
        App.pass = pass;
    }

    public static String getName() {
        return name;
    }

    public static void setName(String name) {
        App.name = name;
    }

    public static String getPort() {
        return port;
    }

    public static void setPort(String port) {
        App.port = port;
    }

    public static String getLocation() {
        return location;
    }

    public static void setLocation(String location) {
        App.location = location;
    }

    private static final OptionParser optionParser = new OptionParser();
    private static final ArgumentAcceptingOptionSpec<String> l = optionParser.accepts("l","(optional) zookeeper location").withRequiredArg().ofType(String.class).defaultsTo("127.0.0.1");
    private static final ArgumentAcceptingOptionSpec<String> u = optionParser.accepts("u","(optional) zookeeper user").withRequiredArg().ofType(String.class).defaultsTo("root");
    private static final ArgumentAcceptingOptionSpec<String> p = optionParser.accepts("p","(optional) zookeeper password").withRequiredArg().ofType(String.class).defaultsTo("secret");
    private static final ArgumentAcceptingOptionSpec<String> n = optionParser.accepts("n","(optional) zookeeper instance name").withRequiredArg().ofType(String.class).defaultsTo("accumulo");
    private static final ArgumentAcceptingOptionSpec<String> z = optionParser.accepts("z","(optional) zookeeper port").withRequiredArg().ofType(String.class).defaultsTo(App.ZOOKEEPER_PROD_PORT);
    private static final ArgumentAcceptingOptionSpec<String> b = optionParser.accepts("b","(optional) musebrowser base server location").withRequiredArg().ofType(String.class).defaultsTo(App.ACCUMULO_REST_SERVER);
    private static final ArgumentAcceptingOptionSpec<String> m = optionParser.accepts("m","(optional) musebrowser port").withRequiredArg().ofType(String.class).defaultsTo(App.ACCUMULO_REST_PORT);
    private static final ArgumentAcceptingOptionSpec<String> s = optionParser.accepts("s","(optional, used when ingesting) source location of *.mini.tar.gz files (e.g., java -jar musebrowser.jar ingest -v \"5.0\" -s \"/Volumes/ram_disk/minis_5.0\")").withRequiredArg().ofType(String.class).defaultsTo(".");
    private static final ArgumentAcceptingOptionSpec<String> t = optionParser.accepts("t","(optional, used when ingesting) target location of *.mini.tar.gz file workspace (e.g., java -jar musebrowser.jar ingest -v \"5.0\" -t \"/myworkspace\")").withRequiredArg().ofType(String.class).defaultsTo(".");
    private static final ArgumentAcceptingOptionSpec<Boolean> i = optionParser.accepts("i","(optional, used when ingesting) log timing instrumentation (defaults to no timing)").withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
    private static final ArgumentAcceptingOptionSpec<String> v = optionParser.accepts("v","(mandatory, used when ingesting) corpus version when ingesting (e.g.,java -jar musebrowser.jar ingest -v \"5.0\")").withRequiredArg().ofType(String.class).defaultsTo("Unknown Version");
    private static final AbstractOptionSpec<Void> h = optionParser.acceptsAll(Arrays.asList("h", "?"),"help (NOTE: When ingesting data add \"ingest\" as the first argument; e.g. java -jar musebrowser.jar ingest -v <version> [other ingest options])").forHelp();

    public static void parseArgs(String[] args){
        OptionSet os = optionParser.parse(args);
        if (os.has(h)){
            try {
                optionParser.printHelpOn(System.out);
            } catch (IOException e) {
                App.logException(e);
            }
            exit(0);
        }

        location = os.valueOf(l);
        user = os.valueOf(u);
        pass = os.valueOf(p);
        name = os.valueOf(n);
        port = os.valueOf(z);
        baseURI = os.valueOf(b);
        musePort = os.valueOf(m);
        currentVersion = os.valueOf(v);
        ScanWrapper.initialize(currentVersion, port);
        sourceDir = os.valueOf(s);
        targetDir = os.valueOf(t);
        injectInstrumentation = os.valueOf(i);
    }

    public static void main(String[] args) {
        try {
            parseArgs(args);
            if (args.length > 0 && args[0].equalsIgnoreCase("ingest")){
                Ingest.main(currentVersion);
                exit(0);
            }
            initialize();

            URI baseUriAndPort = URI.create(new StringBuilder(baseURI).append(":").append(App.musePort).toString());
            final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUriAndPort, resourceConfig, false);
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    server.shutdownNow();
                    teardown();
                }
            }));
            server.start();
            Thread.currentThread().join();
        } catch (IOException | InterruptedException ex) {
            logException(ex);
        }
    }


    public static void initialize(){
        setupCache();
        logger.setUseParentHandlers(false);
        try {
            fh = new FileHandler("./musebrowser.log");
        } catch (IOException e) {
            logException(e);
        }
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        System.out.println("Initializing...");
        ScanWrapper.initialize();
        Security.initialize();
    }
    private static void teardown() {
        provider.close();
    }

    private static void setupCache(){

        provider = Caching.getCachingProvider();
        CacheManager cacheManager = provider.getCacheManager();
        MutableConfiguration<String, Object> configuration =
                new MutableConfiguration<String, Object>()
                        .setTypes(String.class, Object.class)
                        .setStoreByValue(false)
                        .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_DAY));


        resultsCache = cacheManager.createCache("resultsCache", configuration);
    }
}


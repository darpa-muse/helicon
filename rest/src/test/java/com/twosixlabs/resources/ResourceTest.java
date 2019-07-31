package com.twosixlabs.resources;

import com.google.gson.Gson;
import com.twosixlabs.model.accumulo.ScanWrapper;
import com.twosixlabs.muse_utils.App;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.BeforeClass;

import javax.ws.rs.core.Application;

import static junit.framework.TestCase.assertEquals;

public class ResourceTest extends JerseyTest{
    Gson gson = new Gson();

    static boolean ready = false;
    @BeforeClass
    public static void init() {
        // Override zookeeper port to port mapped
        if (!ready) {
            String[] args = {"-z","44001", "-l", "0.0.0.0"};
            App.parseArgs(args);
            App.initialize();
            ready = true;
        }
    }

    @Override
    protected Application configure() {
        return App.resourceConfig;
    }

    protected void testMsg(String msg){
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        System.out.println("Tesing..." + stackTraceElements[2] + " " + msg);
    }
}

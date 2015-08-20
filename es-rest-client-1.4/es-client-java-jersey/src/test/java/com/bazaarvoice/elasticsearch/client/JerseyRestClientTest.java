package com.bazaarvoice.elasticsearch.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JerseyRestClientTest {
    // just in case tests need to instantiate their own clients. The main way to get one is to use the restClient() method.
    protected static final String protocol = "http";
    protected static final String host = "localhost";
    protected static final int port = 9900;

    private static final String esDataDirectory = "/tmp/es-client-java-test-" + UUID.randomUUID();
    private static Node node;
    private static Client client;

    @BeforeSuite
    public static void setup() {
        final Node node = buildNode();
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                node.stop();
            }
        }); // just in case
        JerseyRestClientTest.node = node;

        final ExecutorService executor = Executors.newCachedThreadPool();
        final Client client = JerseyRestClientFactory.client(protocol, host, port, com.sun.jersey.api.client.Client.create(), executor);
        JerseyRestClientTest.client = client;
    }

    private static Node buildNode() {
        final NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();
        nodeBuilder.settings().put("cluster.name", "test cluster");
        nodeBuilder.settings().put("http.port", Integer.toString(port));
        nodeBuilder.settings().put("transport.tcp.port", "9901");
        nodeBuilder.settings().put("network.publish_host", "_local_"); // we're for local testing, don't allow discovery on public ip
        nodeBuilder.settings().put("gateway.type", "none");
        final File homeDirectory = new File(esDataDirectory);
        nodeBuilder.settings().put("path.home", homeDirectory.getPath());
        nodeBuilder.settings().put("path.logs", new File(homeDirectory, "logs").getPath());
        nodeBuilder.settings().put("index.number_of_replicas", 0);
        nodeBuilder.settings().put("script.disable_dynamic", false);
        return nodeBuilder.build();
    }

    protected Client restClient() {
        return client;
    }

    protected Client nodeClient() { return node.client();}

    @AfterSuite
    public void teardown() {
        JerseyRestClientTest.node.stop();
    }
}

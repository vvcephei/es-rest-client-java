import com.bazaarvoice.elasticsearch.client.JerseyHttpClient;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// A sloppy sandbox for doing quick basic tests on the client
// This library is in POC state right now. It's a todo to set up a real testing framework.
public class NonPerm {
    final static Node node = buildNode();

    private static class StartEs {
        public static void main(String[] args) {
            node.start();
        }
    }

    public static void main(String[] args) {

        final ExecutorService executor = Executors.newCachedThreadPool();
        final Client client = JerseyHttpClient.client("http", "localhost", 9800, com.sun.jersey.api.client.Client.create(), executor);

        System.out.println("INDEX");

        final IndexRequestBuilder indexRequestBuilder = client.prepareIndex("testidx", "testtype", "test-id-1").setSource("field", "value");
        final ListenableActionFuture<IndexResponse> execute = indexRequestBuilder.execute();
        final IndexResponse indexResponse = execute.actionGet();
        System.out.println(indexResponse.getIndex());
        System.out.println(indexResponse.getType());
        System.out.println(indexResponse.getId());
        System.out.println(indexResponse.getVersion());
        System.out.println(indexResponse.isCreated());

        System.out.println("GET");

        final GetRequestBuilder getRequestBuilder = client.prepareGet("testidx", "testtype", "test-id-1");
        final ListenableActionFuture<GetResponse> execute1 = getRequestBuilder.execute();
        final GetResponse get = execute1.actionGet();
        System.out.println(get.getIndex());
        System.out.println(get.getType());
        System.out.println(get.getId());
        System.out.println(get.getFields());
        System.out.println(get.getVersion());
        System.out.println(get.getSourceAsString());

        System.out.println("DELETE");

        final DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("testidx", "testtype", "test-id-1");
        final DeleteResponse deleteResponse = deleteRequestBuilder.execute().actionGet();
        System.out.println(deleteResponse.getIndex());
        System.out.println(deleteResponse.getType());
        System.out.println(deleteResponse.getId());
        System.out.println(deleteResponse.getVersion());
        System.out.println(deleteResponse.isFound());

        executor.shutdown();
    }

    private static Node buildNode() {
        final NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();
        nodeBuilder.settings().put("cluster.name", "test cluster");
        nodeBuilder.settings().put("http.port", "9800");
        nodeBuilder.settings().put("transport.tcp.port", "9801");
        nodeBuilder.settings().put("network.publish_host", "_local_"); // we're for local testing, don't allow discovery on public ip
        nodeBuilder.settings().put("gateway.type", "none");
        final File homeDirectory = new File("/tmp/john-test-cluster");
        nodeBuilder.settings().put("path.home", homeDirectory.getPath());
        nodeBuilder.settings().put("path.logs", new File(homeDirectory, "logs").getPath());
        nodeBuilder.settings().put("index.number_of_replicas", 0);
        return nodeBuilder.build();
    }
}

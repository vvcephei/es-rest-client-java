import com.bazaarvoice.elasticsearch.client.JerseyHttpClient;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// A sloppy sandbox for doing quick basic tests on the client
// This library is in POC state right now. It's a todo to set up a real testing framework.
public class HttpClientPlayground {
    private static class StartEs {
        public static void main(String[] args) {
            final Node node = buildNode();
            node.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override public void run() {
                    node.stop();
                }
            });
        }
    }

    public static void main(String[] args) {

        final ExecutorService executor = Executors.newCachedThreadPool();
        final Client client = JerseyHttpClient.client("http", "localhost", 9800, com.sun.jersey.api.client.Client.create(), executor);

        System.out.println("INDEX");

        final IndexRequestBuilder indexRequestBuilder = client.prepareIndex("testidx", "testtype", "test-id-1").setSource("field", "value").setRefresh(true);
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

        System.out.println("SEARCH");

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("testidx");
        searchRequestBuilder.setQuery(QueryBuilders.termQuery("field", "value"));
        searchRequestBuilder.addFacet(FacetBuilders.termsFacet("myfacet").field("field").size(10));
        searchRequestBuilder.addSuggestion(new TermSuggestionBuilder("mysugg").text("valeu").field("field"));
        ListenableActionFuture<SearchResponse> execute2 = searchRequestBuilder.execute();
        SearchResponse searchResponse = execute2.actionGet();
        System.out.println("took: " + Objects.toString(searchResponse.getTook()));
        System.out.println("tookMillis: " + Objects.toString(searchResponse.getTookInMillis()));
        System.out.println("totalShards: " + Objects.toString(searchResponse.getTotalShards()));
        System.out.println("successfulShards: " + Objects.toString(searchResponse.getSuccessfulShards()));
        System.out.println("failedShards: " + Objects.toString(searchResponse.getFailedShards()));
        System.out.println("shardFailures#: " + Objects.toString(searchResponse.getShardFailures().length));
        System.out.println("scrollId: " + Objects.toString(searchResponse.getScrollId()));
        System.out.println("facets: " + Objects.toString(searchResponse.getFacets()));
        final TermsFacet myfacet = searchResponse.getFacets().facet("myfacet");
        System.out.println("facet: name: " + Objects.toString(myfacet.getName()));
        System.out.println("facet: type: " + Objects.toString(myfacet.getType()));
        System.out.println("facet: total: " + Objects.toString(myfacet.getTotalCount()));
        System.out.println("facet: missing: " + Objects.toString(myfacet.getMissingCount()));
        System.out.println("facet: other: " + Objects.toString(myfacet.getOtherCount()));
        for (TermsFacet.Entry entry : myfacet.getEntries()) {
            System.out.println("facet: entry: term: " + Objects.toString(entry.getTerm()));
            System.out.println("facet: entry: count: " + Objects.toString(entry.getCount()));
        }
        System.out.println("facet: other: " + Objects.toString(myfacet.getOtherCount()));
        System.out.println("aggs (not implemented): " + Objects.toString(searchResponse.getAggregations()));
        System.out.println("suggest: " + Objects.toString(searchResponse.getSuggest()));
        System.out.println("maxScore" + Objects.toString(searchResponse.getHits().getMaxScore()));
        System.out.println("totalHits: " + Objects.toString(searchResponse.getHits().getTotalHits()));
        System.out.println("hits: " + Objects.toString(searchResponse.getHits().getHits()));
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println("index: " + Objects.toString(hit.getIndex()));
            System.out.println("shard: " + Objects.toString(hit.getShard()));
            System.out.println("type: " + Objects.toString(hit.getType()));
            System.out.println("id: " + Objects.toString(hit.getId()));
            System.out.println("version: " + Objects.toString(hit.getVersion()));
            System.out.println("source: " + Objects.toString(hit.getSourceAsString()));
            System.out.println("explanation: " + Objects.toString(hit.getExplanation()));
            System.out.println("fields: " + Objects.toString(hit.getFields()));
            System.out.println("highlightFields: " + Objects.toString(hit.getHighlightFields()));
            System.out.println("score: " + Objects.toString(hit.getScore()));
            System.out.println("sortValues#: " + Objects.toString(hit.getSortValues().length));
            System.out.println("matchedQueries#: " + Objects.toString(hit.getMatchedQueries().length));
        }

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

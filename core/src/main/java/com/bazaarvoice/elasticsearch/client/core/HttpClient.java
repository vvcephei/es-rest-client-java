package com.bazaarvoice.elasticsearch.client.core;

import com.bazaarvoice.elasticsearch.client.core.util.InputStreams;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

import static com.bazaarvoice.elasticsearch.client.core.response.IndexResponses.indexResponseCallback;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.booleanToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.longToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.opTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.replicationTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.timeValueToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.versionTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.writeConsistencyLevelToString;
import static org.elasticsearch.common.Preconditions.checkNotNull;
import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.common.base.Optional.of;

public class HttpClient implements Client {
    private static final Optional<String> ABSENT = Optional.absent();

    private final HttpExecutor executor;


    private HttpClient(HttpExecutor executor) {

        this.executor = executor;
    }

    @Override public void close() {

    }

    @Override public AdminClient admin() {
        return null;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(final Action<Request, Response, RequestBuilder> action, final Request request) {
        return null;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(final Action<Request, Response, RequestBuilder> action, final Request request, final ActionListener<Response> listener) {

    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(final Action<Request, Response, RequestBuilder> action) {
        return null;
    }

    @Override public ActionFuture<IndexResponse> index(final IndexRequest request) {
        notNull(request);
        PlainActionFuture<IndexResponse> future = new PlainActionFuture<IndexResponse>();
        index(request, future);
        return future;

    }

    @Override public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        UrlBuilder url = UrlBuilder.create()
            .path(notNull(request.index())).seg(notNull(request.type()))
            .paramIfPresent("routing", fromNullable(request.routing()))
            .paramIfPresent("parent", fromNullable(request.parent()))
            .paramIfPresent("parent", fromNullable(request.parent()))
            .paramIfPresent("timestamp", fromNullable(request.timestamp()))
            .paramIfPresent("ttl", (request.ttl() == -1) ? ABSENT : of(TimeValue.timeValueMillis(request.ttl()).format()))
            .paramIfPresent("timeout", fromNullable(request.timeout()).transform(timeValueToString))
            .paramIfPresent("refresh", fromNullable(request.refresh()).transform(booleanToString))
            .paramIfPresent("version", fromNullable(request.version()).transform(longToString))
            .paramIfPresent("version_type", fromNullable(request.versionType()).transform(versionTypeToString))
            .paramIfPresent("percolate", fromNullable(request.percolate()))
            .paramIfPresent("op_type", fromNullable(request.opType()).transform(opTypeToString))
            .paramIfPresent("replication", fromNullable(request.replicationType()).transform(replicationTypeToString))
            .paramIfPresent("consistency", fromNullable(request.consistencyLevel()).transform(writeConsistencyLevelToString));
        if (request.id() == null) {
            // auto id creation
            ListenableFuture<HttpResponse> post = executor.post(url.url(), InputStreams.of(request.safeSource()));
            Futures.addCallback(post, indexResponseCallback(listener));
        } else {
            ListenableFuture<HttpResponse> put = executor.put(url.seg(request.id()).url(), InputStreams.of(request.safeSource()));
            Futures.addCallback(put, indexResponseCallback(listener));
        }
    }

    @Override public IndexRequestBuilder prepareIndex() {
        return null;
    }

    @Override public ActionFuture<UpdateResponse> update(final UpdateRequest request) {
        return null;
    }

    @Override public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {

    }

    @Override public UpdateRequestBuilder prepareUpdate() {
        return null;
    }

    @Override public UpdateRequestBuilder prepareUpdate(final String index, final String type, final String id) {
        return null;
    }

    @Override public IndexRequestBuilder prepareIndex(final String index, final String type) {
        return null;
    }

    @Override public IndexRequestBuilder prepareIndex(final String index, final String type, @Nullable final String id) {
        return null;
    }

    @Override public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        return null;
    }

    @Override public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {

    }

    @Override public DeleteRequestBuilder prepareDelete() {
        return null;
    }

    @Override public DeleteRequestBuilder prepareDelete(final String index, final String type, final String id) {
        return null;
    }

    @Override public ActionFuture<BulkResponse> bulk(final BulkRequest request) {
        return null;
    }

    @Override public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {

    }

    @Override public BulkRequestBuilder prepareBulk() {
        return null;
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(final DeleteByQueryRequest request) {
        return null;
    }

    @Override public void deleteByQuery(final DeleteByQueryRequest request, final ActionListener<DeleteByQueryResponse> listener) {

    }

    @Override public DeleteByQueryRequestBuilder prepareDeleteByQuery(final String... indices) {
        return null;
    }

    @Override public ActionFuture<GetResponse> get(final GetRequest request) {
        return null;
    }

    @Override public void get(final GetRequest request, final ActionListener<GetResponse> listener) {

    }

    @Override public GetRequestBuilder prepareGet() {
        return null;
    }

    @Override public GetRequestBuilder prepareGet(final String index, @Nullable final String type, final String id) {
        return null;
    }

    @Override public ActionFuture<MultiGetResponse> multiGet(final MultiGetRequest request) {
        return null;
    }

    @Override public void multiGet(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {

    }

    @Override public MultiGetRequestBuilder prepareMultiGet() {
        return null;
    }

    @Override public ActionFuture<CountResponse> count(final CountRequest request) {
        return null;
    }

    @Override public void count(final CountRequest request, final ActionListener<CountResponse> listener) {

    }

    @Override public CountRequestBuilder prepareCount(final String... indices) {
        return null;
    }

    @Override public ActionFuture<SuggestResponse> suggest(final SuggestRequest request) {
        return null;
    }

    @Override public void suggest(final SuggestRequest request, final ActionListener<SuggestResponse> listener) {

    }

    @Override public SuggestRequestBuilder prepareSuggest(final String... indices) {
        return null;
    }

    @Override public ActionFuture<SearchResponse> search(final SearchRequest request) {
        return null;
    }

    @Override public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {

    }

    @Override public SearchRequestBuilder prepareSearch(final String... indices) {
        return null;
    }

    @Override public ActionFuture<SearchResponse> searchScroll(final SearchScrollRequest request) {
        return null;
    }

    @Override public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {

    }

    @Override public SearchScrollRequestBuilder prepareSearchScroll(final String scrollId) {
        return null;
    }

    @Override public ActionFuture<MultiSearchResponse> multiSearch(final MultiSearchRequest request) {
        return null;
    }

    @Override public void multiSearch(final MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener) {

    }

    @Override public MultiSearchRequestBuilder prepareMultiSearch() {
        return null;
    }

    @Override public ActionFuture<SearchResponse> moreLikeThis(final MoreLikeThisRequest request) {
        return null;
    }

    @Override public void moreLikeThis(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {

    }

    @Override public MoreLikeThisRequestBuilder prepareMoreLikeThis(final String index, final String type, final String id) {
        return null;
    }

    @Override public ActionFuture<PercolateResponse> percolate(final PercolateRequest request) {
        return null;
    }

    @Override public void percolate(final PercolateRequest request, final ActionListener<PercolateResponse> listener) {

    }

    @Override public PercolateRequestBuilder preparePercolate(final String index, final String type) {
        return null;
    }

    @Override public ExplainRequestBuilder prepareExplain(final String index, final String type, final String id) {
        return null;
    }

    @Override public ActionFuture<ExplainResponse> explain(final ExplainRequest request) {
        return null;
    }

    @Override public void explain(final ExplainRequest request, final ActionListener<ExplainResponse> listener) {

    }

    @Override public ClearScrollRequestBuilder prepareClearScroll() {
        return null;
    }

    @Override public ActionFuture<ClearScrollResponse> clearScroll(final ClearScrollRequest request) {
        return null;
    }

    @Override public void clearScroll(final ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {

    }

    private <T> T notNull(T t) {
        checkNotNull(t);
        return t;
    }
}

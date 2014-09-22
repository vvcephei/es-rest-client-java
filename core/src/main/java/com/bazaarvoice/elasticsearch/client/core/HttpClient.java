package com.bazaarvoice.elasticsearch.client.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.GetRest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.IndexRest;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.concurrent.Futures;

public class HttpClient extends AbstractClient implements Client {

    private final HttpExecutor executor;


    private HttpClient(HttpExecutor executor) {

        this.executor = executor;
    }

    @Override public void close() {
        // TODO: consider whether we need to close the executor or just let the provider manage its lifecycle independently
    }

    @Override public AdminClient admin() {
        // TODO admin
        return null;
    }

    @Override public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        Futures.addCallback(GetRest.act(executor, request), GetRest.callback(listener));
    }

    @Override public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        Futures.addCallback(IndexRest.act(executor, request), IndexRest.indexResponseCallback(listener));
    }

    @Override public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {

    }


    @Override public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {

    }

    @Override public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {

    }

    @Override public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {

    }

    @Override public void deleteByQuery(final DeleteByQueryRequest request, final ActionListener<DeleteByQueryResponse> listener) {

    }


    @Override public void multiGet(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {

    }

    @Override public void count(final CountRequest request, final ActionListener<CountResponse> listener) {

    }

    @Override public void suggest(final SuggestRequest request, final ActionListener<SuggestResponse> listener) {

    }


    @Override public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {

    }

    @Override public void multiSearch(final MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener) {

    }

    @Override public void moreLikeThis(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {

    }

    @Override public void percolate(final PercolateRequest request, final ActionListener<PercolateResponse> listener) {

    }

    @Override public void explain(final ExplainRequest request, final ActionListener<ExplainResponse> listener) {

    }

    @Override public void clearScroll(final ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {

    }
}

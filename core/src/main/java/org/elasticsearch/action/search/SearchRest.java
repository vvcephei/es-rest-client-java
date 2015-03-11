package org.elasticsearch.action.search;

public class SearchRest {
    // TODO need to revisit this...
    /*public static ListenableFuture<SearchResponse> act(HttpExecutor executor, SearchRequest request) {
        UrlBuilder url = UrlBuilder.create();

        if (request.indices() == null || request.indices().length == 0) {
            url = url.path("_search");
        } else {
            String indices = Joiner.on(',').skipNulls().join(request.indices());
            if (request.types() == null || request.types().length == 0) {
                url = url.path(indices, "_search");
            } else if (request.types() == null || request.types().length == 0) {
                String types = Joiner.on(',').skipNulls().join(request.types());
                url = url.path(indices, types, "_search");
            }
        }

        if (request.extraSource() != null) {
            throw new NotImplementedException();// TODO: implement. not bothering with this for now...
        }

        url = url
            .paramIfPresent("search_type", fromNullable(request.searchType()).transform(searchTypeToString))
            .paramIfPresent("scroll", fromNullable(request.scroll()).transform(scrollToString))
            .paramIfPresent("routing", fromNullable(request.routing()))
            .paramIfPresent("preference", fromNullable(request.preference()))
            .paramIfPresent("ignore_indices", fromNullable(toNullIfDefault(request.ignoreIndices())).transform(ignoreIndicesToString))
        ;


        return Futures.transform(executor.post(url.url(), InputStreams.of(request.source())), searchResponseFunction);
    }

    private static IgnoreIndices toNullIfDefault(final IgnoreIndices ignoreIndices) {
        // TODO add a string serialization for DEFAULT with a PR, then get rid of this method
        if (IgnoreIndices.DEFAULT.equals(ignoreIndices)) {
            return null;
        } else {
            return ignoreIndices;
        }
    }

    public static FutureCallback<SearchResponse> searchResponseCallback(final ActionListener<SearchResponse> listener) {
        return new SearchCallback(listener);
    }

    private static Function<HttpResponse, SearchResponse> searchResponseFunction = new Function<HttpResponse, SearchResponse>() {
        @Override public SearchResponse apply(final HttpResponse httpResponse) {
            try {
                //TODO check REST status and "ok" field and handle failure
                Map<String, Object> map = JsonXContent.jsonXContent.createParser(httpResponse.response()).mapAndClose();
                Map<String, Object> shards = requireMap(map.get("_shards"), String.class, Object.class);
                int totalShards = nodeIntegerValue(shards.get("total"));
                int successfulShards = nodeIntegerValue(shards.get("successful"));
                int failedShards = totalShards - successfulShards;

                InternalFacets facets = null;
                if (map.containsKey("facets")) {

                    final Map<String, Object> facetsMap = requireMap(map.get("facets"), String.class, Object.class);
                    final List<Facet> facetsList = new ArrayList<Facet>(facetsMap.size());
                    for (Map.Entry<String, Object> facetEntry : facetsMap.entrySet()) {
                        final String facetName = facetEntry.getKey();
                        final Map<String, Object> facetMap = requireMap(facetEntry.getValue(), String.class, Object.class);
                        final String type = requireString(facetMap.get("_type"));
                        if (type.equals(DateHistogramFacet.TYPE)) {
                            final List<Object> entries = requireList(facetMap.get("entries"), Object.class);
                            InternalCountDateHistogramFacet.CountEntry[] countEntries = null;
                            InternalFullDateHistogramFacet.FullEntry[] fullEntries = null;
                            for (int i = 0; i < entries.size(); i++) {
                                final Map<String, Object> entryMap = requireMap(entries.get(i), String.class, Object.class);
                                final long time = nodeLongValue(entryMap.get("time"));
                                final long count = nodeLongValue(entryMap.get("count"));
                                final double min = nodeDoubleValue(entryMap.get("min"), Double.NaN);
                                final double max = nodeDoubleValue(entryMap.get("max"), Double.NaN);
                                final double total = nodeDoubleValue(entryMap.get("total"), Double.NaN);
                                final long totalCount = nodeLongValue(entryMap.get("total_count"), 0);
                                final double mean = nodeDoubleValue(entryMap.get("mean"), Double.NaN);
                                if (Double.isNaN(min) && Double.isNaN(max) && Double.isNaN(total) && Double.isNaN(mean) && totalCount == 0) {
                                    checkState(fullEntries == null);
                                    if (countEntries == null) {
                                        countEntries = new InternalCountDateHistogramFacet.CountEntry[entries.size()];
                                    }
                                    countEntries[i] = new InternalCountDateHistogramFacet.CountEntry(time, count);
                                } else {
                                    checkState(countEntries == null);
                                    if (fullEntries == null) {
                                        fullEntries = new InternalFullDateHistogramFacet.FullEntry[entries.size()];
                                    }
                                    fullEntries[i] = new InternalFullDateHistogramFacet.FullEntry(time, count, min, max, totalCount, total);
                                }
                            }
                            final DateHistogramFacet.ComparatorType comparatorType = null; // FIXME not serialized, so there's nothing we can pick here. Not sure of the impact of choosing null.
                            if (countEntries != null) {
                                facetsList.add(new InternalCountDateHistogramFacet(facetName, comparatorType, countEntries));
                            } else {
                                checkState(fullEntries != null);
                                assert fullEntries != null;
                                facetsList.add(new InternalFullDateHistogramFacet(facetName, comparatorType, Arrays.asList(fullEntries)));
                            }
                        } else if (type.equals(HistogramFacet.TYPE)) {
                            final List<Object> entries = requireList(facetMap.get("entries"), Object.class);
                            InternalCountHistogramFacet.CountEntry[] countEntries = null;
                            InternalFullHistogramFacet.FullEntry[] fullEntries = null;
                            for (int i = 0; i < entries.size(); i++) {
                                final Map<String, Object> entryMap = requireMap(entries.get(i), String.class, Object.class);
                                final long key = nodeLongValue(entryMap.get("key"));
                                final long count = nodeLongValue(entryMap.get("count"));
                                final double min = nodeDoubleValue(entryMap.get("min"), Double.NaN);
                                final double max = nodeDoubleValue(entryMap.get("max"), Double.NaN);
                                final double total = nodeDoubleValue(entryMap.get("total"), Double.NaN);
                                final long totalCount = nodeLongValue(entryMap.get("total_count"), 0);
                                final double mean = nodeDoubleValue(entryMap.get("mean"), Double.NaN);
                                if (Double.isNaN(min) && Double.isNaN(max) && Double.isNaN(total) && Double.isNaN(mean) && totalCount == 0) {
                                    checkState(fullEntries == null);
                                    if (countEntries == null) {
                                        countEntries = new InternalCountHistogramFacet.CountEntry[entries.size()];
                                    }
                                    countEntries[i] = new InternalCountHistogramFacet.CountEntry(key, count);
                                } else {
                                    checkState(countEntries == null);
                                    if (fullEntries == null) {
                                        fullEntries = new InternalFullHistogramFacet.FullEntry[entries.size()];
                                    }
                                    fullEntries[i] = new InternalFullHistogramFacet.FullEntry(key, count, min, max, totalCount, total);
                                }
                            }
                            final HistogramFacet.ComparatorType comparatorType = null; // FIXME not serialized, so there's nothing we can pick here. Not sure of the impact of choosing null.
                            if (countEntries != null) {
                                facetsList.add(new InternalCountHistogramFacet(facetName, comparatorType, countEntries));
                            } else {
                                checkState(fullEntries != null);
                                assert fullEntries != null;
                                facetsList.add(new InternalFullHistogramFacet(facetName, comparatorType, Arrays.asList(fullEntries)));
                            }
                        }
                        facets = new InternalFacets(facetsList);
                    }
                    SearchResponse searchResponse = new SearchResponse(
                        new InternalSearchResponse(
                            getSearchHits(map),
                            facets,
                            suggest,
                            nodeBooleanValue(map.get("timed_out"))
                        ),
                        nodeStringValue(map.get("_scroll_id"), null),
                        totalShards,
                        successfulShards,
                        nodeLongValue(map.get("took")),
                        getShardSearchFailures(shards, failedShards));
//                SearchResponse indexResponse = new SearchResponse(
//                    requireString(map.get("_index")),
//                    requireString(map.get("_type")),
//                    requireString(map.get("_id")),
//                    requireLong(map.get("_version")));
//                if (map.containsKey("matches")) {
//                    List<String> matches = requireList(map.get("matches"), String.class);
//                    indexResponse.setMatches(matches);
//                }
//                return indexResponse;
                }catch(IOException e){
                    // FIXME: which exception to use? It should match ES clients if possible.
                    throw new RuntimeException(e);
                }
            }
        }

        ;

        private static ShardSearchFailure[] getShardSearchFailures(final Map<String, Object> shards, final int failedShards) {
            final ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[failedShards];
            Object[] failures = requireList(shards.get("failures"), Object.class).toArray();
            for (int i = 0; i < failedShards; i++) {
                Map<String, Object> failure = requireMap(failures[i], String.class, Object.class);
                SearchShardTarget shard = null;
                if (failure.containsKey("index") && failure.containsKey("shard")) {
                    String index = nodeStringValue(failure.get("index"), null);
                    Integer shardId = nodeIntegerValue(failure.get("shard"));
                    shard = new SearchShardTarget(null, index, shardId);
                }

                shardSearchFailures[i] =
                    new ShardSearchFailure(
                        nodeStringValue(failure.get("reason"), null),
                        shard,
                        findRestStatus(nodeIntegerValue(failure.get("status")))
                    );
            }
            return shardSearchFailures;
        }

        private static InternalSearchHits getSearchHits(final Map<String, Object> map) {
            InternalSearchHits hits = null;
            if (map.containsKey("hits")) {
                Map<String, Object> hitsMap = requireMap(map.get("hits"), String.class, Object.class);
                final long totalHits = nodeLongValue(hitsMap.get("total"));
                final float maxScore = hitsMap.get("max_score") != null ? nodeFloatValue(hitsMap.get("max_score")) : Float.NaN;

                List<InternalSearchHit> internalSearchHits = Lists.newArrayList();
                if (hitsMap.containsKey("hits")) {
                    List<Object> hitsList = requireList(hitsMap.get("hits"), Object.class);
                    for (Object hit : hitsList) {
                        Map<String, Object> hitMap = requireMap(hit, String.class, Object.class);
                        Object explanation = hitMap.get("_explanation");
                        String nodeid = null;
                        int shardid = -1; // FIXME not quite right, but the es serialization node is confusing
                        // it only serializes _shard and _node if explanation != null, but it always serializes _index,
                        // and the only way _index could be set is at the same time as the other fields,
                        // (unless it comes from the read(instream) method, in which case shardid and index get set, but nodeid may be unset.
                        // which suggests that at least index and shardid are set on the ES side, but they are deliberately
                        // leaving shardid off from the serialization when explanation() is null.
                        // If so, there is no way I can recover that information, so I'll just set shardId to an impossible value
                        // TODO send a PR to elasticsearch to fix this on the server side
                        if (explanation != null) {
                            shardid = nodeIntegerValue(hitMap.get("_shard"));
                            nodeid = nodeStringValue(hitMap.get("_node"), null);
                        }
                        String index = nodeStringValue(hitMap.get("_index"), null);
                        SearchShardTarget searchShardTarget = new SearchShardTarget(nodeid, index, shardid);
                        Text type = new StringText(nodeStringValue(hitMap.get("_type"), null));
                        String id = nodeStringValue(hitMap.get("_id"), null);
                        long version = nodeLongValue(hitMap.get("_version"), -1);
                        float score = nodeFloatValue(hitMap.get("_score"), Float.NaN);
                        BytesReference source = readBytesReference(hitMap.get("_source"));
                        final int docId = -1; // this field isn't serialized
                        ImmutableMap.Builder<String, SearchHitField> fields = ImmutableMap.builder();
                        if (hitMap.containsKey("fields")) {
                            Map<String, Object> fieldsMap = requireMap(hitMap.get("fields"), String.class, Object.class);
                            for (Map.Entry<String, Object> fieldEntry : fieldsMap.entrySet()) {
                                final ImmutableList.Builder<Object> valuesBuilder = ImmutableList.builder();
                                if (fieldEntry.getValue() instanceof List) {
                                    for (Object value : requireList(fieldEntry.getValue(), Object.class)) {
                                        valuesBuilder.add(value);
                                    }
                                } else {
                                    valuesBuilder.add(fieldEntry.getValue());
                                }
                                SearchHitField field = new InternalSearchHitField(fieldEntry.getKey(), valuesBuilder.build());
                                fields.put(field.getName(), field);
                            }
                        }
                        InternalSearchHit internalSearchHit = new InternalSearchHit(docId, id, type, source, fields.build());

                        internalSearchHit.shardTarget(searchShardTarget);

                        if (hitMap.containsKey("highlight")) {
                            ImmutableMap.Builder<String, HighlightField> highlights = ImmutableMap.builder();
                            final Map<String, Object> highlightMap = requireMap(hitMap.get("highlight"), String.class, Object.class);
                            for (Map.Entry<String, Object> entry : highlightMap.entrySet()) {
                                final String name = entry.getKey();
                                final Text[] fragments;
                                if (entry.getValue() == null) {
                                    fragments = null;
                                } else {
                                    final List<String> strings = requireList(entry.getValue(), String.class);
                                    fragments = new Text[strings.size()];
                                    for (int i = 0; i < strings.size(); i++) {
                                        fragments[i] = new StringText(strings.get(i));
                                    }
                                }
                                final HighlightField highlightField = new HighlightField(name, fragments);
                                highlights.put(highlightField.getName(), highlightField);
                            }
                            internalSearchHit.highlightFields(highlights.build());
                        }

                        if (hitMap.containsKey("sort")) {
                            // TODO: can't really tell if this is the right thing to do.
                            final List<Object> sorts = requireList(hitMap.get("sort"), Object.class);
                            internalSearchHit.sortValues(sorts.toArray());
                        }

                        if (hitMap.containsKey("matched_filters")) {
                            final List<String> matched_filters = requireList(hitMap.get("matched_filters"), String.class);
                            internalSearchHit.matchedQueries(matched_filters.toArray(new String[matched_filters.size()]));
                        }

                        if (explanation != null) {
                            internalSearchHit.explanation(getExplanation(explanation));
                        }

                        internalSearchHits.add(internalSearchHit);
                    }
                }
                hits = new InternalSearchHits(internalSearchHits.toArray(new InternalSearchHit[internalSearchHits.size()]), totalHits, maxScore);
            }
            return hits;
        }

        private static Explanation getExplanation(final Object explanationObj) {
            final Map<String, Object> explainMap = requireMap(explanationObj, String.class, Object.class);
            final float value = nodeFloatValue(explainMap.get("value"));
            final String description = nodeStringValue(explainMap.get("description"), null);
            final Explanation explanation = new Explanation(value, description);
            if (explainMap.containsKey("details")) {
                for (Object detail : requireList(explainMap.get("details"), Object.class)) {
                    explanation.addDetail(getExplanation(detail));
                }
            }
            return explanation;
        }


        private static class SearchCallback implements FutureCallback<SearchResponse> {
            private final ActionListener<SearchResponse> listener;

            private SearchCallback(ActionListener<SearchResponse> listener) {
                this.listener = listener;
            }

            @Override public void onSuccess(final SearchResponse indexResponse) {
                listener.onResponse(indexResponse);
            }

            @Override public void onFailure(final Throwable throwable) {
                // TODO transform failure
                listener.onFailure(throwable);
            }
        }
    }*/
}

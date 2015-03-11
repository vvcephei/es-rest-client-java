package com.bazaarvoice.elasticsearch.client.core.util;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.Scroll;

public class StringFunctions {
    public static final Function<IndexRequest.OpType, String> opTypeToString = new Function<IndexRequest.OpType, String>() {
        @Override public String apply(final IndexRequest.OpType o) {
            switch (o) {
                case CREATE:
                    return "create";
                case INDEX:
                    return "index";
                default:
                    throw new IllegalStateException(String.format("unexpected op type, %s", o));
            }
        }
    };
    public static final Function<TimeValue, String> timeValueToString = new Function<TimeValue, String>() {
        @Override public String apply(final TimeValue timeValue) {
            return Long.toString(timeValue.millis());
        }
    };


    public static final Function<Boolean, String> booleanToString = new Function<Boolean, String>() {
        @Override public String apply(final Boolean aBoolean) {
            return aBoolean.toString();
        }
    };
    public static final Function<Long, String> longToString = new Function<Long, String>() {
        @Override public String apply(final Long aLong) {
            return aLong.toString();
        }
    };
    public static final Function<String[], String> commaDelimitedToString = new Function<String[], String>() {
        @Override public String apply(final String[] strings) {
            return Joiner.on(',').skipNulls().join(strings);
        }
    };

    public static final Function<Scroll, String> scrollToString = new Function<Scroll, String>() {
        @Override public String apply(final Scroll scroll) {
            return scroll.keepAlive().format();
        }
    };

    // TODO send PR. These should be methods on the enums.

    public static final Function<VersionType, String> versionTypeToString = new Function<VersionType, String>() {
        @Override public String apply(final VersionType versionType) {
            switch (versionType) {
                case EXTERNAL:
                    return "external";
                case INTERNAL:
                    return "internal";
                default:
                    throw new IllegalStateException(String.format("unexpected version type %s", versionType));
            }
        }
    };
    public static final Function<ReplicationType, String> replicationTypeToString = new Function<ReplicationType, String>() {
        @Override public String apply(final ReplicationType replicationType) {
            switch (replicationType) {
                case ASYNC:
                    return "async";
                case SYNC:
                    return "sync";
                case DEFAULT:
                    return "default";
                default:
                    throw new IllegalStateException(String.format("unexpected replication type %s", replicationType));
            }
        }
    };
    public static final Function<WriteConsistencyLevel, String> writeConsistencyLevelToString = new Function<WriteConsistencyLevel, String>() {
        @Override public String apply(final WriteConsistencyLevel writeConsistencyLevel) {
            switch (writeConsistencyLevel) {
                case ALL:
                    return "all";
                case DEFAULT:
                    return "default";
                case QUORUM:
                    return "quorum";
                case ONE:
                    return "one";
                default:
                    throw new IllegalStateException(String.format("unexpected write consistency level %s", writeConsistencyLevel));
            }
        }
    };

    public static final Function<SearchType, String> searchTypeToString = new Function<SearchType, String>() {
        @Override public String apply(final SearchType searchType) {
            switch (searchType) {
                case COUNT:
                    return "count";
                case DFS_QUERY_AND_FETCH:
                    return "dfs_query_and_fetch";
                case DFS_QUERY_THEN_FETCH:
                    return "dfs_query_then_fetch";
                case QUERY_AND_FETCH:
                    return "query_and_fetch";
                case QUERY_THEN_FETCH:
                    return "query_then_fetch";
                case SCAN:
                    return "scan";
                default:
                    throw new IllegalStateException(String.format("unexpected search type %s", searchType));
            }
        }
    };


}

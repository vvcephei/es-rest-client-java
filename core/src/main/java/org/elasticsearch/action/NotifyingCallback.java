package org.elasticsearch.action;

import org.elasticsearch.common.util.concurrent.FutureCallback;

public class NotifyingCallback<ResponseType> implements FutureCallback<ResponseType> {
    private final ActionListener<ResponseType> listener;

    public NotifyingCallback(final ActionListener<ResponseType> listener) {
        this.listener = listener;
    }

    @Override public void onSuccess(final ResponseType response) {
        listener.onResponse(response);
    }

    @Override public void onFailure(final Throwable throwable) {
        // TODO transform failure
        listener.onFailure(throwable);
    }
}
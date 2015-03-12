package org.elasticsearch.action.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.FutureCallback;

public class DeleteCallback implements FutureCallback<DeleteResponse> {
    private final ActionListener<DeleteResponse> listener;

    public DeleteCallback(final ActionListener<DeleteResponse> listener) {

        this.listener = listener;
    }

    @Override public void onSuccess(final DeleteResponse deleteResponse) {
        listener.onResponse(deleteResponse);
    }

    @Override public void onFailure(final Throwable throwable) {
        //TODO transform failure
        listener.onFailure(throwable);
    }
}

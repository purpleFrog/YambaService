package com.twitter.university.android.yamba.service;

import android.accounts.AccountManager;
import android.app.IntentService;
import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.res.Resources;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

import com.marakana.android.yamba.clientlib.YambaClient;
import com.marakana.android.yamba.clientlib.YambaClientException;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class YambaService extends IntentService {
    private static final String TAG = "SVC";

    private static final int NOTIFICATION_ID = 7;
    private static final int NOTIFICATION_INTENT_ID = 13;

    private static final String PARAM_TOKEN = "PARAM_TOKEN";

    private static final String IS_NULL = " is null ";
    private static final String IS_EQ = "=?";

    public static void sync(Context ctxt, String token) {
        Log.d(TAG, "sync: " + token);
        Intent i = new Intent(ctxt, YambaService.class);
        i.putExtra(YambaContract.Service.PARAM_OP, YambaContract.Service.OP_SYNC);
        i.putExtra(PARAM_TOKEN, token);
        ctxt.startService(i);
    }

    public static void startPoller(Context ctxt) {
        Intent i = new Intent(ctxt, YambaService.class);
        i.putExtra(YambaContract.Service.PARAM_OP, YambaContract.Service.OP_START_POLLING);
        ctxt.startService(i);
    }

    public static void stopPoller(Context ctxt) {
        Intent i = new Intent(ctxt, YambaService.class);
        i.putExtra(YambaContract.Service.PARAM_OP, YambaContract.Service.OP_STOP_POLLING);
        ctxt.startService(i);
    }


    private volatile int pollSize;

    public YambaService() { super(TAG); }

    @Override
    public void onCreate() {
        super.onCreate();
        if (BuildConfig.DEBUG) { Log.d(TAG, "created"); }

        pollSize = getResources().getInteger(R.integer.poll_size);
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        if (BuildConfig.DEBUG) { Log.d(TAG, "destroyed"); }
    }

    @Override
    protected void onHandleIntent(Intent i) {
        int op = i.getIntExtra(YambaContract.Service.PARAM_OP, 0);
        if (BuildConfig.DEBUG) { Log.d(TAG, "exec: " + op); }
        switch (op) {
            case YambaContract.Service.OP_SYNC:
                doSync(i.getStringExtra(PARAM_TOKEN));
            default:
                Log.e(TAG, "Unexpected op: " + op);
        }
    }

    private void doSync(String token) {
        if (BuildConfig.DEBUG) { Log.d(TAG, "sync"); }

        YambaClient client = null;
        try { client = ((YambaApplication) getApplication()).getYambaClient(); }
        catch (YambaClientException e) {
            Log.e(TAG, "failed getting client", e);
        }
        if (null == client) {
            Log.e(TAG, "Client is null");
            return;
        }

        try { postPending(client); }
        catch (YambaClientException e) {
            Log.e(TAG, "post failed", e);
        }

        try { notifyTimelineUpdate(parseTimeline(client.getTimeline(pollSize))); }
        catch (YambaClientException e) {
            Log.e(TAG, "poll failed", e);
            e.printStackTrace();
        }
    }

    private int postPending(YambaClient client) throws YambaClientException {
        ContentResolver cr = getContentResolver();

        String xactId = UUID.randomUUID().toString();

        ContentValues row = new ContentValues();
        row.put(YambaContract.Posts.Columns.TRANSACTION, xactId);

        int n = cr.update(
            YambaContract.Posts.URI,
            row,
            YambaContract.Posts.Columns.SENT + IS_NULL
                + "AND " + YambaContract.Posts.Columns.TRANSACTION + IS_NULL,
            null);

        if (BuildConfig.DEBUG) { Log.d(TAG, "pending: " + n); }
        if (0 >= n) { return 0; }

        Cursor cur = null;
        try {
            cur = cr.query(
                YambaContract.Posts.URI,
                null,
                YambaContract.Posts.Columns.TRANSACTION + IS_EQ,
                new String[] { xactId },
                YambaContract.Posts.Columns.TIMESTAMP + " ASC");
            return postTweets(cur, client);
        }
        finally {
            if (null != cur) { cur.close(); }
            row.clear();
            row.putNull(YambaContract.Posts.Columns.TRANSACTION);
            cr.update(
                YambaContract.Posts.URI,
                row,
                YambaContract.Posts.Columns.TRANSACTION + IS_EQ,
                new String[] { xactId });
        }
    }

    private int postTweets(Cursor c, YambaClient client) throws YambaClientException {
        int idIdx = c.getColumnIndex(YambaContract.Posts.Columns.ID);
        int tweetIdx = c.getColumnIndex(YambaContract.Posts.Columns.TWEET);

        int n = 0;
        ContentValues row = new ContentValues();
        while (c.moveToNext()) {
            client.postStatus(c.getString(tweetIdx));
            row.clear();
            row.put(YambaContract.Posts.Columns.SENT, System.currentTimeMillis());
            Uri uri = YambaContract.Posts.URI.buildUpon().appendPath(c.getString(idIdx)).build();
            n += getContentResolver().update(uri, row, null, null);
        }
        return n;
    }

    private int parseTimeline(List<YambaClient.Status> timeline) {
        long latest = getLatestTweetTime();
        if (BuildConfig.DEBUG) { Log.d(TAG, "latest: " + latest); }

        List<ContentValues> vals = new ArrayList<ContentValues>();

        for (YambaClient.Status tweet: timeline) {
            long t = tweet.getCreatedAt().getTime();
            if (t <= latest) { continue; }

            ContentValues cv = new ContentValues();
            cv.put(YambaContract.Timeline.Columns.ID, Long.valueOf(tweet.getId()));
            cv.put(YambaContract.Timeline.Columns.TIMESTAMP, Long.valueOf(t));
            cv.put(YambaContract.Timeline.Columns.HANDLE, tweet.getUser());
            cv.put(YambaContract.Timeline.Columns.TWEET, tweet.getMessage());
            vals.add(cv);
        }

        int n = vals.size();
        if (0 >= n) { return 0; }
        n = getContentResolver().bulkInsert(
            YambaContract.Timeline.URI,
            vals.toArray(new ContentValues[n]));

        if (BuildConfig.DEBUG) { Log.d(TAG, "inserted: " + n); }
        return n;
    }

    private long getLatestTweetTime() {
        Cursor c = null;
        try {
            c = getContentResolver().query(
                YambaContract.Timeline.URI,
                new String[] { "max(" + YambaContract.Timeline.Columns.TIMESTAMP + ")" },
                null,
                null,
                null);
            return ((null == c) || (!c.moveToNext()))
                ? Long.MIN_VALUE
                : c.getLong(0);
        }
        finally {
            if (null != c) { c.close(); }
        }
    }


    private void notifyTimelineUpdate(int count) {
        Intent i = new Intent(YambaContract.Service.ACTION_TIMELINE_UPDATED);
        i.putExtra(YambaContract.Service.PARAM_COUNT, count);
        if (BuildConfig.DEBUG) { Log.d(TAG, "timeline: " + count); }
        sendBroadcast(i, YambaContract.Service.PERMISSION_RECEIVE_TIMELINE_UPDATE);
    }
}


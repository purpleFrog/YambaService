package com.twitter.university.android.yamba.data;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;

import com.twitter.university.android.yamba.service.YambaContract;


public class YambaProvider extends ContentProvider {
    private static final String TAG = "PROVIDER";

    private static final int POST_ITEM_TYPE = 1;
    private static final int POST_DIR_TYPE = 2;
    private static final int TIMELINE_ITEM_TYPE = 3;
    private static final int TIMELINE_DIR_TYPE = 4;

    //  scheme                     authority                   path  [id]
    // content://com.twitter.university.android.yamba.timeline/timeline/7
    private static final UriMatcher MATCHER = new UriMatcher(UriMatcher.NO_MATCH);
    static {
        MATCHER.addURI(
            YambaContract.AUTHORITY,
            YambaContract.Posts.TABLE + "/#",
            POST_ITEM_TYPE);
        MATCHER.addURI(
            YambaContract.AUTHORITY,
            YambaContract.Posts.TABLE,
            POST_DIR_TYPE);
        MATCHER.addURI(
            YambaContract.AUTHORITY,
            YambaContract.Timeline.TABLE + "/#",
            TIMELINE_ITEM_TYPE);
        MATCHER.addURI(
            YambaContract.AUTHORITY,
            YambaContract.Timeline.TABLE,
            TIMELINE_DIR_TYPE);
    }

    private YambaDbHelper dbHelper;

    @Override
    public boolean onCreate() {
        Log.d(TAG, "created");
        dbHelper = new YambaDbHelper(getContext());
        return dbHelper != null;
    }

    @Override
    public String getType(Uri uri) {
        switch (MATCHER.match(uri)) {
            case POST_ITEM_TYPE:
                return YambaContract.Posts.ITEM_TYPE;
            case POST_DIR_TYPE:
                return YambaContract.Posts.DIR_TYPE;
            case TIMELINE_ITEM_TYPE:
                return YambaContract.Timeline.ITEM_TYPE;
            case TIMELINE_DIR_TYPE:
                return YambaContract.Timeline.DIR_TYPE;
            default:
                return null;
        }
    }

    @SuppressWarnings("fallthrough")
    @Override
    public Cursor query(Uri uri, String[] proj, String sel, String[] selArgs, String sort) {
        Log.d(TAG, "query");

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();

        switch (MATCHER.match(uri)) {
            case POST_ITEM_TYPE:
                qb.appendWhere(YambaContract.Posts.Columns.ID + "=" + ContentUris.parseId(uri));
            case POST_DIR_TYPE:
                qb.setTables(YambaContract.Posts.TABLE);
                break;
            case TIMELINE_ITEM_TYPE:
                qb.appendWhere(YambaContract.Timeline.Columns.ID + "=" + ContentUris.parseId(uri));
            case TIMELINE_DIR_TYPE:
                qb.setTables(YambaContract.Timeline.TABLE);
                break;
            default:
                throw new IllegalArgumentException("Unexpected uri: " + uri);
        }

        Cursor c = qb.query(getDb(), proj, sel, selArgs, null, null, sort);

        c.setNotificationUri(getContext().getContentResolver(), uri);

        return c;
    }

    @Override
    public int bulkInsert(Uri uri, ContentValues[] rows) {
        Log.d(TAG, "insert: " + rows.length);

        String table;
        switch (MATCHER.match(uri)) {
            case TIMELINE_DIR_TYPE:
                table = YambaContract.Timeline.TABLE;
                break;
            default:
                throw new IllegalArgumentException("Unexpected uri: " + uri);
        }

        int count = 0;

        SQLiteDatabase db = getDb();
        try {
            db.beginTransaction();
            for (ContentValues row: rows) {
                if (0 < db.insert(table, null, row)) { count++; }
            }
            db.setTransactionSuccessful();
        }
        finally {
            db.endTransaction();
        }

        if (0 < count) {
            getContext().getContentResolver().notifyChange(uri, null, false);
        }

        return count;
    }

    @Override
    public Uri insert(Uri uri, ContentValues row) {
        switch (MATCHER.match(uri)) {
            case POST_DIR_TYPE:
                break;
            default:
                throw new IllegalArgumentException("Unexpected uri: " + uri);
        }

        row.put(YambaContract.Posts.Columns.TIMESTAMP, System.currentTimeMillis());

        long id = getDb().insert(YambaContract.Posts.TABLE, null, row);

        if (0 >= id) { return null; }

        uri = uri.buildUpon().appendPath(String.valueOf(id)).build();
        getContext().getContentResolver().notifyChange(uri, null, true);

        return uri;
    }

    @Override
    public int update(Uri uri, ContentValues row, String sel, String[] selArgs) {
        switch (MATCHER.match(uri)) {
            case POST_ITEM_TYPE:
                sel = appendWhere(sel, YambaContract.Posts.Columns.ID + "=" + ContentUris.parseId(uri));
            case POST_DIR_TYPE:
                break;
            default:
                throw new IllegalArgumentException("Unexpected uri: " + uri);
        }

        int n = getDb().update(YambaContract.Posts.TABLE, row, sel, selArgs);

        if (0 < n) {
            getContext().getContentResolver().notifyChange(uri, null, false);
        }

        return n;
    }

    @Override
    public int delete(Uri arg0, String arg1, String[] arg2) {
        throw new UnsupportedOperationException("delete not supported");
    }

    private String appendWhere(String sel, String cond) {
        return (TextUtils.isEmpty(sel))
            ? cond
            : new StringBuilder("(").append(cond)
                .append(") AND (").append(sel).append(")").toString();
    }

    private SQLiteDatabase getDb() { return dbHelper.getWritableDatabase(); }
}

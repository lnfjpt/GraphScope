package com.alibaba.graphscope.ldbc.workload;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import okhttp3.internal.Util;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final class Encoder {
  public Encoder(byte[] bs) {
    this.bs = bs;
    this.loc = 0;
  }

  public static int serialize_long(byte[] bytes, int offset, long value) {
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    return offset;
  }

  public static int serialize_int(byte[] bytes, int offset, int value) {
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    value >>= 8;
    bytes[offset++] = (byte) (value & 0xFF);
    return offset;
  }

  public static int serialize_byte(byte[] bytes, int offset, byte value) {
    bytes[offset++] = value;
    return offset;
  }

  public static int serialize_bytes(byte[] bytes, int offset, byte[] value) {
    offset = serialize_int(bytes, offset, value.length);
    System.arraycopy(value, 0, bytes, offset, value.length);
    return offset + value.length;
  }

  public static int serialize_raw_bytes(byte[] bytes, int offset, byte[] value) {
    System.arraycopy(value, 0, bytes, offset, value.length);
    return offset + value.length;
  }

  public static byte[] serialize_long_byte(long value, byte type) {
    byte[] bytes = new byte[9];
    serialize_long(bytes, 0, value);
    serialize_byte(bytes, 8, type);
    return bytes;
  }

  public static byte[] serialize_long_long_byte(long a, long b, byte type) {
    byte[] bytes = new byte[17];
    serialize_long(bytes, 0, a);
    serialize_long(bytes, 8, b);
    serialize_byte(bytes, 16, type);
    return bytes;
  }

  public static byte[] serialize_long_int_byte(long a, int b, byte type) {
    byte[] bytes = new byte[13];
    serialize_long(bytes, 0, a);
    serialize_int(bytes, 8, b);
    serialize_byte(bytes, 12, type);
    return bytes;
  }

  public static byte[] serialize_long_long_long_byte(long a, long b, long c, byte type) {
    byte[] bytes = new byte[25];
    serialize_long(bytes, 0, a);
    serialize_long(bytes, 8, b);
    serialize_long(bytes, 16, c);
    serialize_byte(bytes, 24, type);
    return bytes;
  }

  public static byte[] serialize_long_long_int_byte(long a, long b, int c, byte type) {
    byte[] bytes = new byte[21];
    serialize_long(bytes, 0, a);
    serialize_long(bytes, 8, b);
    serialize_int(bytes, 16, c);
    serialize_byte(bytes, 20, type);
    return bytes;
  }

  public static byte[] serialize_long_string_byte(long value, String b, byte type) {
    byte[] b_ba = b.getBytes();
    byte[] bytes = new byte[13 + b_ba.length];
    serialize_long(bytes, 0, value);
    int offset = serialize_bytes(bytes, 8, b_ba);
    serialize_byte(bytes, offset, type);
    return bytes;
  }

  public void put_int(int value) {
    this.loc = serialize_int(this.bs, this.loc, value);
  }

  public void put_byte(byte value) {
    this.loc = serialize_byte(this.bs, this.loc, value);
  }

  public void put_long(long value) {
    this.loc = serialize_long(this.bs, this.loc, value);
  }

  public void put_bytes(byte[] bytes) {
    this.loc = serialize_bytes(this.bs, this.loc, bytes);
  }

  byte[] bs;
  int loc;
}

public class InteractiveClient {
  static int READ_TIMEOUT = 5000000;
  static int CONNECTION_TIMEOUT = 5000000;
  static int CONNECTION_POOL_MAX_IDLE = 128;
  static int KEEP_ALIVE_DURATION = 5000;
  static int MAX_REQUESTS_PER_HOST = 180;
  static int MAX_REQUESTS = 180;

  private OkHttpClient client = null;
  final private String uri;
  final private String updateUri;

  public InteractiveClient(List<String> endpoints) {
    assert (endpoints.size() == 1);
    String[] host = endpoints.get(0).split(":");
    client = new OkHttpClient.Builder()
        .dispatcher(new Dispatcher(new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            Util.threadFactory("OkHttp Dispatcher", false))))
        .connectionPool(new ConnectionPool(CONNECTION_POOL_MAX_IDLE,
            KEEP_ALIVE_DURATION,
            TimeUnit.MILLISECONDS))
        .readTimeout(READ_TIMEOUT, TimeUnit.MILLISECONDS)
        .connectTimeout(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
        .build();
    client.dispatcher().setMaxRequests(MAX_REQUESTS);
    client.dispatcher().setMaxRequestsPerHost(MAX_REQUESTS_PER_HOST);
    String serverAddr = String.format("http://%s:%s/", host[0], host[1]);
    uri = serverAddr + "/interactive/query";
    updateUri = serverAddr + "/interactive/update";
  }

  public byte[] syncPost(byte[] parameters) throws IOException {
    RequestBody body = RequestBody.create(parameters);
    Request request = new Request.Builder()
        .url(uri)
        .post(body)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new IOException("Unexpected code " + response);
      }
      return response.body().bytes();
    }
  }

  public void syncPostWithoutReply(byte[] parameters) throws IOException {
    RequestBody body = RequestBody.create(parameters);
    Request request = new Request.Builder()
        .url(updateUri)
        .post(body)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new IOException("Unexpected code " + response);
      }
    }
  }
}
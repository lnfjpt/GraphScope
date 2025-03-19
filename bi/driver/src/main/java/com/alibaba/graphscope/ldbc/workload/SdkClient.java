package com.alibaba.graphscope.ldbc.workload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.client.common.Config;
import com.alibaba.graphscope.interactive.models.*;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

public class SdkClient {
  public static final long DEFAULT_READ_TIMEOUT = 20000000;
  public static final long DEFAULT_WRITE_TIMEOUT = 20000000;

  private Session session;
  private Driver driver;
  private String graphId;

  public SdkClient(String endpoint) {
    driver = Driver.connect(endpoint);
    Config config = new Config.ConfigBuilder().readTimeout(DEFAULT_READ_TIMEOUT).writeTimeout(DEFAULT_WRITE_TIMEOUT).build();
    session = driver.session(config);
  }

  public byte[] callProcedure(String query_name, List<String> arguments) {
    List<String> request_list = new ArrayList<>();
    request_list.add(query_name);
    for (String argument : arguments) {
      request_list.add(argument);
    }
    ObjectMapper mapper = new ObjectMapper();
    byte[] jsonBytes = new byte[0];
    try {
      jsonBytes = mapper.writeValueAsBytes(request_list);
    } catch (IOException e) {
      e.printStackTrace();
    }
    Result<byte[]> resp = session.callProcedureRaw(jsonBytes);
    if (resp.isOk()) {
      return resp.getValue();
    } else {
      return null;
    }
  }
}

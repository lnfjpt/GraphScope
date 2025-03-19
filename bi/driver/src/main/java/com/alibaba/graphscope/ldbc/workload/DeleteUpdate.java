package com.alibaba.graphscope.ldbc.workload;

import com.alibaba.graphscope.ldbc.workload.proto.Common;
import com.alibaba.graphscope.ldbc.workload.proto.StoredProcedure;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.graphscope.ldbc.workload.proto.IrResult;
import com.alibaba.graphscope.ldbc.workload.proto.GraphAlgebraPhysical.PhysicalPlan;
import com.alibaba.graphscope.ldbc.workload.proto.GraphAlgebraPhysical.PhysicalPlan.Builder;
import com.alibaba.pegasus.common.StreamIterator;
import com.google.protobuf.util.JsonFormat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

public class DeleteUpdate {
  static String DELETE_QUERY_VARIANTS[] = {"deletePerson", "deleteForum", "deleteComment", "deletePost"};
  static String SPLIT_DELIMITER = "\\|";
  private static final ObjectMapper mapper = new ObjectMapper();

  public static CSVParser readCSV(String filePath) throws IOException {
    Reader reader = new FileReader(filePath);
    CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().withSkipHeaderRecord().withDelimiter('|').parse(reader);
    return parser;
  }

  public static CSVParser readCSVGZ(String compressedFilePath) throws IOException {
    InputStream fileStream = new FileInputStream(compressedFilePath);
    InputStream gzipStream = new GZIPInputStream(fileStream);
    Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
    CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().withSkipHeaderRecord().withDelimiter('|').parse(decoder);
    return parser;
  }

  public static void run_queries(List<String> endpoints, String rawDataPath, String outputDirPath, String batchId) throws Exception {
    int startIndex = 0;
    try {
      List<String> personParameters = new ArrayList<String>();
      List<String> forumParameters = new ArrayList<String>();
      List<String> postParameters = new ArrayList<String>();
      List<String> commentParameters = new ArrayList<String>();
      // Run PERSON queries
      {
        String personInput = String.format("%s/deletes/dynamic/Person/batch_id=%s/", rawDataPath, batchId);
        FilenameFilter filter = (file, name) -> name.endsWith(".csv") || name.endsWith(".csv.gz");
        File inputDir = new File(personInput);
        File[] files = inputDir.listFiles(filter);
        if (files != null) {
          for (File file : files) {
            if (file.getName().endsWith(".csv.gz")) {
              for (CSVRecord record : readCSVGZ(personInput + file.getName())) {
                personParameters.add(record.get("deletionDate") + "|" + record.get("id"));
              }
            } else if (file.getName().endsWith(".csv")) {
              for (CSVRecord record : readCSV(personInput + file.getName())) {
                personParameters.add(record.get("deletionDate") + "|" + record.get("id"));
              }
            }
          }
        }
        List<String> message_result = run_query("deletePersonMessage", endpoints, personParameters);
        String commentPath = String.format("%s/%s/Comment.csv", outputDirPath, batchId);
        String postPath = String.format("%s/%s/Post.csv", outputDirPath, batchId);
        FileWriter commentWriter = new FileWriter(commentPath, true);
        FileWriter postWriter = new FileWriter(postPath, true);
        commentWriter.write("deletionDate|id\n");
        postWriter.write("deletionDate|id\n");
        for (String message : message_result) {
          if (message.endsWith("|2")) {
            commentParameters.add(message.split("\\|")[0] + "|" + message.split("\\|")[1]);
            commentWriter.write(message.substring(0, message.length() -2) + "\n");
          } else if (message.endsWith("|3")) {
            postParameters.add(message.split("\\|")[0] + "|" +message.split("\\|")[1]);
            postWriter.write(message.substring(0, message.length() -2) + "\n");
          }
        }
        commentWriter.close();
        postWriter.close();

        List<String> forum_result = run_query("deletePersonForum", endpoints, personParameters);
        String forumPath = String.format("%s/%s/Forum.csv", outputDirPath, batchId);
        FileWriter forumWriter = new FileWriter(forumPath, true);
        forumWriter.write("deletionDate|id\n");
        for (String forum : forum_result) {
            forumParameters.add(forum.split("\\|")[0] + "|" +forum.split("\\|")[1]);
            forumWriter.write(forum + "\n");
        }
        forumWriter.close();
      }

      // Forum
      {
        String forumInput = String.format("%s/deletes/dynamic/Forum/batch_id=%s/", rawDataPath, batchId);
        FilenameFilter filter = (file, name) -> name.endsWith(".csv") || name.endsWith(".csv.gz");
        File inputDir = new File(forumInput);
        File[] files = inputDir.listFiles(filter);
        if (files != null) {
          for (File file : files) {
            if (file.getName().endsWith(".csv.gz")) {
              for (CSVRecord record : readCSVGZ(forumInput + file.getName())) {
                forumParameters.add(record.get("deletionDate") + "|" + record.get("id"));
              }
            } else if (file.getName().endsWith(".csv")) {
              for (CSVRecord record : readCSV(forumInput + file.getName())) {
                forumParameters.add(record.get("deletionDate") + "|" + record.get("id"));
              }
            }
          }
        }
        List<String> post_result = run_query("deleteForum", endpoints, forumParameters);
        String postPath = String.format("%s/%s/Post.csv", outputDirPath, batchId);
        FileWriter postWriter = new FileWriter(postPath, true);
        for (String post : post_result) {
          postParameters.add(post.split("\\|")[0] + "|" +post.split("\\|")[1]);
          postWriter.write(post + "\n");
        }
        postWriter.close();
      }

      // Post
      {
        String postInput = String.format("%s/deletes/dynamic/Post/batch_id=%s/", rawDataPath, batchId);
        FilenameFilter filter = (file, name) -> name.endsWith(".csv") || name.endsWith(".csv.gz");
        File inputDir = new File(postInput);
        File[] files = inputDir.listFiles(filter);
        if (files != null) {
          for (File file : files) {
            if (file.getName().endsWith(".csv.gz")) {
              for (CSVRecord record : readCSVGZ(postInput + file.getName())) {
                postParameters.add(record.get("deletionDate") + "|" + record.get("id"));
              }
            } else if (file.getName().endsWith(".csv")) {
              for (CSVRecord record : readCSV(postInput + file.getName())) {
                postParameters.add(record.get("deletionDate") + "|" + record.get("id"));
              }
            }
          }
        }
        List<String> comment_result = run_query("deletePost", endpoints, postParameters);
        String commentPath = String.format("%s/%s/Comment.csv", outputDirPath, batchId);
        FileWriter commentWriter = new FileWriter(commentPath, true);
        for (String comment : comment_result) {
          commentWriter.write(comment + "\n");
        }
        commentWriter.close();
      }

      // Comment
      {
        String commentInput = String.format("%s/deletes/dynamic/Comment/batch_id=%s/", rawDataPath, batchId);
        FilenameFilter filter = (file, name) -> name.endsWith(".csv") || name.endsWith(".csv.gz");
        File inputDir = new File(commentInput);
        File[] files = inputDir.listFiles(filter);
        if (files != null) {
          for (File file : files) {
            if (file.getName().endsWith(".csv.gz")) {
              for (CSVRecord record : readCSVGZ(commentInput + file.getName())) {
                commentParameters.add(record.get("deletionDate") + "|" + record.get("id"));
              }
            } else if (file.getName().endsWith(".csv")) {
              for (CSVRecord record : readCSV(commentInput + file.getName())) {
                commentParameters.add(record.get("deletionDate") + "|" + record.get("id"));
              }
            }
          }
        }
        List<String> comment_result = run_query("deleteComment", endpoints, commentParameters);
        String commentPath = String.format("%s/%s/Comment.csv", outputDirPath, batchId);
        FileWriter commentWriter = new FileWriter(commentPath, true);
        for (String comment : comment_result) {
          commentParameters.add(comment.split("\\|")[0] + "|" + comment.split("\\|")[1]);
          commentWriter.write(comment + "\n");
        }
        commentWriter.close();
      }
    } catch (IOException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
    }
  }


  public static List<String> run_query(String queryVariant, List<String> endpoints, List<String> parameters) throws Exception {
    BIClient biClient = new BIClient(endpoints);

    List<String> results_list = new ArrayList<>();
    for (String query : parameters) {
      Map<String, String> map = new HashMap<>();
      String[] split = query.split("\\|");
      if (split.length < 2) {
	 continue;
      }
      String vertexId = split[1];
      String deleteDate = split[0];
      map.put("id", vertexId);
      StreamIterator<String> results = biClient.submit(queryVariant, map);
      while (results.hasNext()) {
        String result = results.next();
        result = deleteDate + "|" + result;
        results_list.add(result);
      }
    }
    return results_list;
  }

  public static void main(String[] args) throws Exception {
    String rawDataPath = System.getProperty("raw");
    String endpoints = System.getProperty("endpoint");
    String engine = System.getProperty("engine");
    String outputDir = System.getProperty("output");
    String batchId = System.getProperty("batch_id");

    if (rawDataPath == null) {
      throw new RuntimeException("raw data dir is not set");
    }
    if (endpoints == null) {
      throw new RuntimeException("endpoint dir is not set");
    }
    if (outputDir == null) {
      throw new RuntimeException("output dir is not set");
    }
    if (batchId == null) {
      throw new RuntimeException("batch id is not set");
    }

    String tempDirName = String.format("%s/extra_deletes/", outputDir);
    File outputTempDir = new File(tempDirName);
    outputTempDir.mkdir();
    File batchDir = new File(tempDirName + batchId);
    batchDir.mkdir();

    List<String> endpointList = new ArrayList();
    for (String endpoint : endpoints.split(";")) {
      endpointList.add(endpoint);
    }

    run_queries(endpointList, rawDataPath, tempDirName, batchId);
  }
}

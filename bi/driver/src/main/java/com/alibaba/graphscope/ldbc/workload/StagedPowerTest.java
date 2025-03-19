package com.alibaba.graphscope.ldbc.workload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class StagedPowerTest {
    static String POWER_TEST_BATCH = "2012-11-29";
    static String UPDATE_VARIANTS[] = {"insert_comment", "insert_forum", "insert_person", "insert_post", "insert_comment_hasCreator_person", "insert_comment_hasTag_tag",
            "insert_comment_isLocatedIn_country", "insert_comment_replyOf_comment", "insert_comment_replyOf_post", "insert_forum_containerOf_post", "insert_forum_hasMember_person",
            "insert_forum_hasModerator_person", "insert_forum_hasTag_tag", "insert_person_hasInterest_tag", "insert_person_isLocatedIn_city", "insert_person_knows_person",
            "insert_person_likes_comment", "insert_person_likes_post", "insert_person_studyAt_university", "insert_person_workAt_company", "insert_post_hasCreator_person",
            "insert_post_hasTag_tag", "insert_post_isLocatedIn_country", "delete_comment", "delete_forum", "delete_person_message", "delete_person_forum", "delete_post",
            "delete_comment_hasCreator_person", "delete_comment_isLocatedIn_country", "delete_comment_replyOf_comment", "delete_comment_replyOf_post", "delete_forum_containerOf_post",
            "delete_forum_hasMember_person", "delete_forum_hasModerator_person", "delete_person_isLocatedIn_city", "delete_person_knows_person", "delete_person_likes_comment",
            "delete_person_likes_post", "delete_post_hasCreator_person", "delete_post_isLocatedIn_country"};
    static String PRECOMPUTE_VARIANTS[] = {"bi4_precompute", "bi6_precompute", "interaction1_count_precompute", "interaction2_count_precompute", "comment_root_precompute", "bi20_precompute"};
    static String QUERY_VARIANTS1[] = {"1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b"};
    static String QUERY_VARIANTS2[] = {"11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"};
    // static String QUERY_VARIANTS2[] = {"15b"};
    static String SPLIT_DELIMITER = "\\|";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static String getUpdateType(String query_name) {
        String[] parts = query_name.split("_");
        return parts[0] + "s";
    }

    public static String getDirName(String query_name) {
        String[] parts = query_name.split("_");
        StringBuilder dirNameOutput = new StringBuilder();

        if (query_name == "delete_person_message" || query_name == "delete_person_forum") {
            return "Person";
        }
        if (parts.length == 2) {
            dirNameOutput.append(parts[1].substring(0, 1).toUpperCase()).append(parts[1].substring(1));
        } else if (parts.length == 4) {
            dirNameOutput.append(parts[1].substring(0, 1).toUpperCase()).append(parts[1].substring(1));
            dirNameOutput.append("_");
            dirNameOutput.append(parts[2]);
            dirNameOutput.append("_");
            dirNameOutput.append(parts[3].substring(0, 1).toUpperCase()).append(parts[3].substring(1));
        }
        return dirNameOutput.toString();
    }

    public static List<Long> runBatchUpdate(List<SdkClient> clients, String rawDataPath, String batchId) throws Exception {
        List<Long> durations = new ArrayList();
        for (String updateQuery : UPDATE_VARIANTS) {
            System.out.println(String.format("========================= %s =========================", updateQuery));
            long startTime = System.currentTimeMillis();
            String updateType = getUpdateType(updateQuery);
            String dirName = getDirName(updateQuery);
            String filePath = String.format("%s/%s/dynamic/%s/batch_id=%s/*.csv.gz", rawDataPath, updateType, dirName, batchId);
            List<String> paramsList = new ArrayList(Arrays.asList(filePath));
            int numberOfCalls = clients.size();
            ExecutorService executor = Executors.newFixedThreadPool(numberOfCalls);
            Future<byte[]>[] futures = new Future[numberOfCalls];
            for (int i = 0; i < clients.size(); i++) {
                final int id = i;
                futures[i] = executor.submit(new Callable<byte[]>() {
                    @Override
                    public byte[] call() throws Exception {
                        byte[] results = clients.get(id).callProcedure(updateQuery, paramsList);
                        return results;
                    }
                });
            }

            List<byte[]> resultsList = new ArrayList<>();
            for (int i = 0; i < numberOfCalls; i++) {
                try {
                    resultsList.add(futures[i].get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            executor.shutdown();

            byte[] results = resultsList.get(0);
            long endTime = System.currentTimeMillis();
            durations.add(endTime - startTime);
        }
        return durations;
    }

    public static List<Long> runPrecompute(List<SdkClient> clients) throws Exception {
        List<Long> durations = new ArrayList();
        for (String precomputeQuery : PRECOMPUTE_VARIANTS) {
            System.out.println(String.format("========================= %s =========================", precomputeQuery));
            long startTime = System.currentTimeMillis();
            List<String> paramsList = new ArrayList();
            int numberOfCalls = clients.size();
            ExecutorService executor = Executors.newFixedThreadPool(numberOfCalls);
            Future<byte[]>[] futures = new Future[numberOfCalls];
            for (int i = 0; i < clients.size(); i++) {
                final int id = i;
                futures[i] = executor.submit(new Callable<byte[]>() {
                    @Override
                    public byte[] call() throws Exception {
                        byte[] results = clients.get(id).callProcedure(precomputeQuery, paramsList);
                        return results;
                    }
                });
            }

            List<byte[]> resultsList = new ArrayList<>();
            for (int i = 0; i < numberOfCalls; i++) {
                try {
                    resultsList.add(futures[i].get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            executor.shutdown();

            byte[] results = resultsList.get(0);
            long endTime = System.currentTimeMillis();
            durations.add(endTime - startTime);
        }
        return durations;
    }

    public static List<Long> runQuery(String queryVariant, List<SdkClient> clients, List<QueryAndResult.Query> parameters, int startId, String outputDir) throws Exception {
        List<Long> durations = new ArrayList<Long>();
        int count = 0;
        FileWriter resultsWriter = new FileWriter(outputDir + "/results.csv", true);

        for (QueryAndResult.Query query : parameters) {
            // Build new parameter here
            // run 30 queries
            if (count == 30) {
                break;
            }
            ++count;

            resultsWriter.write(queryVariant + "|" + mapper.writeValueAsString(query) + "|");

            List<String> paramsList = query.parameterList();
            System.out.println("parameter: " + queryVariant + " " + query.type() + " " + query.toString());

            long startTime = System.currentTimeMillis();
            int numberOfCalls = clients.size();
            ExecutorService executor = Executors.newFixedThreadPool(numberOfCalls);
            Future<byte[]>[] futures = new Future[numberOfCalls];
            for (int i = 0; i < clients.size(); i++) {
                final int id = i;
                futures[i] = executor.submit(new Callable<byte[]>() {
                    @Override
                    public byte[] call() throws Exception {
                        byte[] results = clients.get(id).callProcedure("bi" + queryVariant, paramsList);
                        return results;
                    }
                });
            }

            List<byte[]> resultsList = new ArrayList<>();
            for (int i = 0; i < numberOfCalls; i++) {
                try {
                    resultsList.add(futures[i].get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            executor.shutdown();

            byte[] results = resultsList.get(0);
            List<Object> results_list = new ArrayList<>();
            ByteBuffer buffer = ByteBuffer.wrap(results);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            while (buffer.remaining() > 8) {
                long stringLength = buffer.getLong();
                byte[] stringBytes = new byte[(int) stringLength];
                buffer.get(stringBytes);
                String result = new String(stringBytes, java.nio.charset.StandardCharsets.UTF_8);
                System.out.println(result);
                List<String> values = new ArrayList<>(List.of(result.split(SPLIT_DELIMITER)));
                results_list.add(query.parseResult(values));
            }
            resultsWriter.write(mapper.writeValueAsString(results_list) + "\n");

            long endTime = System.currentTimeMillis();
            durations.add(endTime - startTime);
            startId += 1;
        }
        resultsWriter.close();
        return durations;
    }

    public static void runPowerTest0(int sf, List<SdkClient> clients, String rawDataPath, String outputDir) throws Exception {
        try {
            long writeStartTime = System.currentTimeMillis();
            List<Long> updateDurations = runBatchUpdate(clients, rawDataPath, POWER_TEST_BATCH);
            List<Long> precomputeDurations = runPrecompute(clients);
            long writeEndTime = System.currentTimeMillis();
            long writeTime = writeEndTime - writeStartTime;
	    System.out.println(String.format("write takes: %d ms", writeTime));
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    public static void runPowerTest1(int sf, Map<String, List<QueryAndResult.Query>> parametersMap, List<SdkClient> clients, String rawDataPath, String outputDir) throws Exception {
	int startIndex = 0;
        try {
            File timeFile = new File(String.format("%s/time-1.csv", outputDir));
            if (timeFile.createNewFile()) {
                System.out.println("File created: " + timeFile.getName());
            } else {
                System.out.println("File already exists.");
            }

            List<Long> readTimes = new ArrayList<>();
            FileWriter timeWriter = new FileWriter(String.format("%s/time-1.csv", outputDir));
            for (String queryVariant : QUERY_VARIANTS1) {
                System.out.println(String.format("========================= Q%s =========================", queryVariant));
                List<QueryAndResult.Query> queries = parametersMap.get(queryVariant);
                String queryName = queryVariant.replace("a", "").replace("b", "");
                List<Long> durations = runQuery(queryName, clients, queries, startIndex, outputDir);
                long readTime = 0;
                for (int i = 0; i < durations.size(); i++) {
                    readTime += durations.get(i);
                    timeWriter.write(String.format("%s,%d,%d\n", queryVariant, startIndex + i, durations.get(i)));
                }
                readTimes.add(readTime);
                startIndex += durations.size();
            }
            timeWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    public static void runPowerTest2(int sf, Map<String, List<QueryAndResult.Query>> parametersMap, List<SdkClient> clients, String rawDataPath, String outputDir) throws Exception {
	int startIndex = 0;
        try {
            File timeFile = new File(String.format("%s/time-2.csv", outputDir));
            if (timeFile.createNewFile()) {
                System.out.println("File created: " + timeFile.getName());
            } else {
                System.out.println("File already exists.");
            }

            List<Long> readTimes = new ArrayList<>();
            FileWriter timeWriter = new FileWriter(String.format("%s/time-2.csv", outputDir));
            for (String queryVariant : QUERY_VARIANTS2) {
                System.out.println(String.format("========================= Q%s =========================", queryVariant));
                List<QueryAndResult.Query> queries = parametersMap.get(queryVariant);
                String queryName = queryVariant.replace("a", "").replace("b", "");
                List<Long> durations = runQuery(queryName, clients, queries, startIndex, outputDir);
                long readTime = 0;
                for (int i = 0; i < durations.size(); i++) {
                    readTime += durations.get(i);
                    timeWriter.write(String.format("%s,%d,%d\n", queryVariant, startIndex + i, durations.get(i)));
                }
                readTimes.add(readTime);
                startIndex += durations.size();
            }
            timeWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    public static Map<String, List<QueryAndResult.Query>> loadBenchmarkParams1(String parametersPath) throws Exception {
        Map<String, List<QueryAndResult.Query>> parametersMap = new HashMap<>();
        for (String query_variant : QUERY_VARIANTS1) {
            List<QueryAndResult.Query> records = new ArrayList<>();
            String parameterName = String.format("%s/bi-%s.csv", parametersPath, query_variant);
            try (BufferedReader br = new BufferedReader(new FileReader(parameterName))) {
                String line;
                boolean first_line = true;
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(SPLIT_DELIMITER);
                    if (first_line) {
                        first_line = false;
                        continue;
                    }
                    System.out.println(QueryAndResult.generateOperation(Arrays.asList(values), query_variant));
                    records.add(QueryAndResult.generateOperation(Arrays.asList(values), query_variant));

                }
            }
            parametersMap.put(query_variant, records);
        }
        return parametersMap;
    }

    public static Map<String, List<QueryAndResult.Query>> loadBenchmarkParams2(String parametersPath) throws Exception {
        Map<String, List<QueryAndResult.Query>> parametersMap = new HashMap<>();
        for (String query_variant : QUERY_VARIANTS2) {
            List<QueryAndResult.Query> records = new ArrayList<>();
            String parameterName = String.format("%s/bi-%s.csv", parametersPath, query_variant);
            try (BufferedReader br = new BufferedReader(new FileReader(parameterName))) {
                String line;
                boolean first_line = true;
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(SPLIT_DELIMITER);
                    if (first_line) {
                        first_line = false;
                        continue;
                    }
                    System.out.println(QueryAndResult.generateOperation(Arrays.asList(values), query_variant));
                    records.add(QueryAndResult.generateOperation(Arrays.asList(values), query_variant));

                }
            }
            parametersMap.put(query_variant, records);
        }
        return parametersMap;
    }

    class Server {
        private String host;
        private int port;

        public String getEndpoint() {
            return host + ":" + port;
        }
    }

    class ServersConfig {
        private List<Server> servers;

        public List<Server> getServers() {
            return servers;
        }
    }

    public static void main(String[] args) throws Exception {
        String scaleFactor = System.getProperty("SF");
        String rawDataPath = System.getProperty("raw_data");
        String parametersPath = System.getProperty("parameters");
        String endpoint = System.getProperty("endpoint");
        String workersString = System.getProperty("workers");
	String stageString = System.getProperty("stage");

        if (scaleFactor == null) {
            throw new RuntimeException("scale factor is not set");
        }
        if (rawDataPath == null) {
            throw new RuntimeException("raw data dir is not set");
        }
        if (parametersPath == null) {
            throw new RuntimeException("parameters dir is not set");
        }
        if (endpoint == null) {
            throw new RuntimeException("endpoint path is not set");
        }
        if (workersString == null) {
            throw new RuntimeException("workers is not set");
        }
	if (stageString == null) {
            throw new RuntimeException("stage is not set");
	}

        boolean validation = false;
        boolean isPowerTest = true;
        System.out.println(String.format("Start to run throughput test - stage - %s", stageString));

        int sf = Integer.parseInt(scaleFactor);
        int workers = Integer.parseInt(workersString);
        File outputRootDir = new File("output");
        outputRootDir.mkdir();
        String outputDirName = String.format("output/output-sf%d-%s", sf, stageString);
        File outputDir = new File(outputDirName);
        if (outputDir.mkdir()) {
            System.out.println(String.format("Output dir \"%s\" created", outputDir.toString()));
        } else {
            System.out.println("Fail to create output dir");
        }

        Gson gson = new Gson();
        List<SdkClient> clients = new ArrayList<SdkClient>();
        try (FileReader reader = new FileReader(endpoint)) {
            ServersConfig serversConfig = gson.fromJson(reader, ServersConfig.class);
            for (Server server : serversConfig.getServers()) {
                String ep = server.getEndpoint();
                clients.add(new SdkClient(ep));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        long benchmarkStart = System.currentTimeMillis();
	if (stageString.equals("0")) {
        	runPowerTest0(sf, clients, rawDataPath, outputDirName);
	} else if (stageString.equals("1")) {
        	Map<String, List<QueryAndResult.Query>> parametersMap =
                	loadBenchmarkParams1(parametersPath);
        	runPowerTest1(sf, parametersMap, clients, rawDataPath, outputDirName);
	} else if (stageString.equals("2")) {
        	Map<String, List<QueryAndResult.Query>> parametersMap =
                	loadBenchmarkParams2(parametersPath);
        	runPowerTest2(sf, parametersMap, clients, rawDataPath, outputDirName);
	} else {
		System.out.println(String.format("unknown stage - %s", stageString));
	}
        long benchmarkEnd = System.currentTimeMillis();
        long benchmarkDuration = benchmarkEnd - benchmarkStart;
        System.out.println(String.format("Benchmark stage - %s time: %dms", stageString, benchmarkDuration));
    }
}

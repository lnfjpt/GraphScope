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

public class Benchmark {
    static String POWER_TEST_BATCH = "2012-11-29";
    static String BATCH_LIST[] = {"2012-11-29", "2012-11-30", "2012-12-01", "2012-12-02", "2012-12-03", "2012-12-04", "2012-12-05", "2012-12-06", "2012-12-07",
            "2012-12-08", "2012-12-09", "2012-12-10", "2012-12-11", "2012-12-12", "2012-12-13", "2012-12-14", "2012-12-15", "2012-12-16", "2012-12-17", "2012-12-18",
            "2012-12-19", "2012-12-20", "2012-12-21", "2012-12-22", "2012-12-23", "2012-12-24", "2012-12-25", "2012-12-26", "2012-12-27", "2012-12-28", "2012-12-29",
            "2012-12-30", "2012-12-31"};
    static String UPDATE_VARIANTS[] = {"insert_comment", "insert_forum", "insert_person", "insert_post", "insert_comment_hasCreator_person", "insert_comment_hasTag_tag",
            "insert_comment_isLocatedIn_country", "insert_comment_replyOf_comment", "insert_comment_replyOf_post", "insert_forum_containerOf_post", "insert_forum_hasMember_person",
            "insert_forum_hasModerator_person", "insert_forum_hasTag_tag", "insert_person_hasInterest_tag", "insert_person_isLocatedIn_city", "insert_person_knows_person",
            "insert_person_likes_comment", "insert_person_likes_post", "insert_person_studyAt_university", "insert_person_workAt_company", "insert_post_hasCreator_person",
            "insert_post_hasTag_tag", "insert_post_isLocatedIn_country", "delete_comment", "delete_forum", "delete_person_message", "delete_person_forum", "delete_post",
            "delete_comment_hasCreator_person", "delete_comment_isLocatedIn_country", "delete_comment_replyOf_comment", "delete_comment_replyOf_post", "delete_forum_containerOf_post",
            "delete_forum_hasMember_person", "delete_forum_hasModerator_person", "delete_person_isLocatedIn_city", "delete_person_knows_person", "delete_person_likes_comment",
            "delete_person_likes_post", "delete_post_hasCreator_person", "delete_post_isLocatedIn_country"};
    static String PRECOMPUTE_VARIANTS[] = {"bi4_precompute", "bi6_precompute", "interaction1_count_precompute", "interaction2_count_precompute", "comment_root_precompute", "bi20_precompute"};
    static String QUERY_VARIANTS[] = {"1", "2a", "2b", "3", "4", "5", "6", "7", "8a", "8b", "9", "10a", "10b", "11", "12", "13", "14a", "14b", "15a", "15b", "16a", "16b", "17", "18", "19a", "19b", "20a", "20b"};
    static String SPLIT_DELIMITER = "\\|";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static List<Map.Entry<QueryAndResult.Query, String>> load_validation_params(String parametersPath) throws IOException {
        List<Map.Entry<QueryAndResult.Query, String>> validationSet = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(parametersPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(SPLIT_DELIMITER);
                QueryAndResult.Query query = QueryAndResult.deserializeOperation(values[2], values[0]);
                System.out.println(mapper.writeValueAsString(query.deserializeResult(values[3])));
                validationSet.add(new AbstractMap.SimpleEntry<QueryAndResult.Query, String>(query, values[3]));
            }
        }
        return validationSet;
    }

    public static Map<String, List<QueryAndResult.Query>> loadBenchmarkParams(String parametersPath) throws Exception {
        Map<String, List<QueryAndResult.Query>> parametersMap = new HashMap<>();
        for (String query_variant : QUERY_VARIANTS) {
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

    public static List<Map.Entry<QueryAndResult.Query, String>> loadValidationParams(String parametersPath)
            throws IOException {
        List<Map.Entry<QueryAndResult.Query, String>> validationSet = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(parametersPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(SPLIT_DELIMITER);
                QueryAndResult.Query query = QueryAndResult.deserializeOperation(values[2], values[0]);
                validationSet.add(new AbstractMap.SimpleEntry<QueryAndResult.Query, String>(query, values[3]));
            }
        }
        return validationSet;
    }

    public static void runValidationTest(List<Map.Entry<QueryAndResult.Query, String>> parametersMaps, List<SdkClient> clients, String rawDataPath, String outputDir) throws Exception {
        int startIndex = 0;
        List<Long> updateDurations = runBatchUpdate(clients, rawDataPath, POWER_TEST_BATCH, startIndex);
        startIndex += updateDurations.size();
        List<Long> precomputeDurations = runPrecompute(clients, startIndex);
        startIndex += precomputeDurations.size();
        int failed_count = 0;

        boolean failed = false;
        FileWriter expected_output = null, actual_output = null;

        try {
            expected_output = new FileWriter(outputDir + "/" + "validation-params-failed-expected.json");
            actual_output = new FileWriter(outputDir + "/" + "validation-params-failed-actual.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Map<Integer, Integer> correctMap = new HashMap<>();
        Map<Integer, Integer> totalMap = new HashMap<>();
        for (Map.Entry<QueryAndResult.Query, String> entry : parametersMaps) {
            QueryAndResult.Query query = entry.getKey();
            String value = entry.getValue();
            String queryVariant = String.valueOf(query.type());
            List<String> paramsList = query.parameterList();

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
            List<Object> resultsActual = new ArrayList<>();
            ByteBuffer buffer = ByteBuffer.wrap(results);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            while (buffer.remaining() > 8) {
                long stringLength = buffer.getLong();
                byte[] stringBytes = new byte[(int) stringLength];
                buffer.get(stringBytes);
                String result = new String(stringBytes, java.nio.charset.StandardCharsets.UTF_8);
                System.out.println(result);
                List<String> values = new ArrayList<>(List.of(result.split(SPLIT_DELIMITER)));
                resultsActual.add(query.parseResult(values));
            }
            failed = false;
            List<Object> resultsExpected = null;
            try {
                resultsExpected = query.deserializeResult(value);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (resultsActual.size() != resultsExpected.size()) {
                failed = true;
            } else {
                for (int i = 0; i < resultsActual.size(); i++) {
                    if (!resultsExpected.get(i).equals(resultsActual.get(i))) {
                        failed = true;
                        break;
                    }
                }
            }


            if (failed) {
                expected_output.write(String.valueOf(query.type()) + "|" + mapper.writeValueAsString(query) + "|"
                        + mapper.writeValueAsString(resultsExpected) + "\n");
                actual_output.write(String.valueOf(query.type()) + "|" + mapper.writeValueAsString(query) + "|"
                        + mapper.writeValueAsString(resultsActual) + "\n");

                failed_count++;
            } else {
                if (!correctMap.containsKey(query.type())) correctMap.put(query.type(), 1);
                else correctMap.put(query.type(), correctMap.get(query.type()) + 1);
            }
            if (!totalMap.containsKey(query.type())) totalMap.put(query.type(), 1);
            else totalMap.put(query.type(), totalMap.get(query.type()) + 1);
        }
        expected_output.close();
        actual_output.close();
        int padRightDistance = 15;
        StringBuilder sb = new StringBuilder();
        sb.append("Validation Result: ").append((failed_count != 0) ? "FAIL" : "PASS").append("\n");
        sb.append("  ***\n");
        sb.append("  Correct results for ").append(parametersMaps.size() - failed_count)
                .append(" operations\n");
        for (int operationType = 1; operationType <= 20; ++operationType) {
            sb.append("    ").
                    append((correctMap.containsKey(operationType))
                            ? correctMap.get(operationType)
                            : "0")
                    .append(" / ").
                    append(String.format("%1$-" + padRightDistance + "s",
                            totalMap.containsKey(operationType) ? totalMap.get(operationType) : "0")).
                    append("bi" + operationType).
                    append("\n");
        }
        sb.append("  Incorrect results for ").append(failed_count)
                .append(" operations\n");
        System.out.println(sb);
    }

    public static void runPowerTest(int sf, Map<String, List<QueryAndResult.Query>> parametersMap, List<SdkClient> clients, String rawDataPath, String outputDir) throws Exception {
        int startIndex = 0;

        try {
            File timeFile = new File(String.format("%s/time.csv", outputDir));
            if (timeFile.createNewFile()) {
                System.out.println("File created: " + timeFile.getName());
            } else {
		FileWriter timeWriter = new FileWriter(String.format("%s/time.csv", outputDir));
		timeWriter.close();
                System.out.println("File already exists.");
            }

            long writeStartTime = System.currentTimeMillis();
            List<Long> updateDurations = runBatchUpdate(clients, rawDataPath, POWER_TEST_BATCH, startIndex);
            startIndex += updateDurations.size();
            List<Long> precomputeDurations = runPrecompute(clients, startIndex);
            startIndex += precomputeDurations.size();
            long writeEndTime = System.currentTimeMillis();
            long writeTime = writeEndTime - writeStartTime;

            List<Long> readTimes = new ArrayList<>();
            for (String queryVariant : QUERY_VARIANTS) {
                System.out.println(String.format("========================= Q%s =========================", queryVariant));
                List<QueryAndResult.Query> queries = parametersMap.get(queryVariant);
                String queryName = queryVariant.replace("a", "").replace("b", "");
                List<Long> durations = runQuery(queryName, clients, queries, startIndex, outputDir);
                FileWriter timeWriter = new FileWriter(String.format("%s/time.csv", outputDir), true);
                long readTime = 0;
                for (int i = 0; i < durations.size(); i++) {
                    readTime += durations.get(i);
                    timeWriter.write(String.format("%s,%d,%d\n", queryVariant, startIndex + i, durations.get(i)));
                }
                readTimes.add(readTime);
                startIndex += durations.size();
                timeWriter.close();
            }
            calcPowerScore(sf, readTimes, writeTime);
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    public static void runThroughputTest(Map<String, List<QueryAndResult.Query>> parametersMap, List<SdkClient> clients, String rawDataPath, String outputDir) throws Exception {
        int startIndex = 0;

        try {
            File timeFile = new File(String.format("%s/time.csv", outputDir));
            if (timeFile.createNewFile()) {
                System.out.println("File created: " + timeFile.getName());
            } else {
                System.out.println("File already exists.");
            }

            List<Long> batchTimes = new ArrayList<>();
            for (String batch_id : BATCH_LIST) {
                long batchStartTime = System.currentTimeMillis();
                List<Long> updateDurations = runBatchUpdate(clients, rawDataPath, POWER_TEST_BATCH, startIndex);
                startIndex += updateDurations.size();
                List<Long> precomputeDurations = runPrecompute(clients, startIndex);
                startIndex += precomputeDurations.size();

                FileWriter timeWriter = new FileWriter(String.format("%s/time.csv", outputDir));
                for (String queryVariant : QUERY_VARIANTS) {
                    System.out.println(String.format("========================= Q%s =========================", queryVariant));
                    List<QueryAndResult.Query> queries = parametersMap.get(queryVariant);
                    String queryName = queryVariant.replace("a", "").replace("b", "");
                    List<Long> durations = runQuery(queryName, clients, queries, startIndex, outputDir);
                    for (int i = 0; i < durations.size(); i++) {
                        timeWriter.write(String.format("%s,%d,%d\n", queryVariant, startIndex + i, durations.get(i)));
                    }
                    startIndex += durations.size();
                }
                long batchEndTime = System.currentTimeMillis();
                long batchTime = batchEndTime - batchStartTime;
                batchTimes.add(batchTime);
                timeWriter.close();
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    public static void calcPowerScore(int sf, List<Long> readTimes, long writeTime) throws Exception {
        double product = (double) writeTime / 1000;
        for (long readTime : readTimes) {
            product = product * ((double) readTime / 1000);
        }
        double powerScore = 3600 * sf / Math.pow(product, 1.0 / 29);
        System.out.println(String.format("Power score of benchmark is %f", powerScore));
    }

    public static void calcThroughputScore(int sf, double load_time, List<Long> batchTimes) throws Exception {
        double totalBatchTime = 0;
        for (long batchTime : batchTimes) {
            totalBatchTime += ((double) batchTime / 1000 / 3600);
        }
        double throughputScore = sf * (24 - load_time) * batchTimes.size() / totalBatchTime;
        System.out.println(String.format("Power score of benchmark is %f", throughputScore));
    }

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

    public static List<Long> runBatchUpdate(List<SdkClient> clients, String rawDataPath, String batchId, int startId) throws Exception {
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
            startId += 1;
        }
        return durations;
    }

    public static List<Long> runPrecompute(List<SdkClient> clients, int startId) throws Exception {
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
            startId += 1;
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
        String validationString = System.getProperty("validation");
        String queryMode = System.getProperty("mode");

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
        boolean validation = false;
        if (validationString != null &&
                (validationString.equals("true") || validationString.equals("1"))) {
            validation = true;
        }
        boolean isPowerTest = true;
        if (queryMode != null && queryMode.equals("throughput")) {
            isPowerTest = false;
            System.out.println("Start to run throughput test");
        } else if (queryMode != null && queryMode.equals("power")) {
            System.out.println("Start to run power test");
        } else {
            System.out.println("Unknown query mode, start to run power test");
        }

        int sf = Integer.parseInt(scaleFactor);
        int workers = Integer.parseInt(workersString);
        File outputRootDir = new File("output");
        outputRootDir.mkdir();
        String outputDirName = String.format("output/output-sf%d", sf);
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

        if (validation) {
            List<Map.Entry<QueryAndResult.Query, String>> parametersMap = loadValidationParams(parametersPath);
            runValidationTest(parametersMap, clients, rawDataPath, outputDirName);
        } else {
            Map<String, List<QueryAndResult.Query>> parametersMap =
                    loadBenchmarkParams(parametersPath);
            long benchmarkStart = System.currentTimeMillis();
            if (isPowerTest) {
                runPowerTest(sf, parametersMap, clients, rawDataPath, outputDirName);
                long benchmarkEnd = System.currentTimeMillis();
                long benchmarkDuration = benchmarkEnd - benchmarkStart;
                System.out.println(String.format("Benchmark time: %dms", benchmarkDuration));
            } else {
                runThroughputTest(parametersMap, clients, rawDataPath, outputDirName);
                long benchmarkEnd = System.currentTimeMillis();
                long benchmarkDuration = benchmarkEnd - benchmarkStart;
                System.out.println(String.format("Benchmark time: %dms", benchmarkDuration));
            }
        }
    }
}
package com.alibaba.graphscope.ldbc.workload;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.DEDUCTION;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class QueryAndResult {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    // private static Map<Class<?>, Class<? extends Query>> queryMap = new HashMap<>();
    private static Map<String, Class<? extends Query>> queryMap = new HashMap<>();

    static {
        queryMap.put("1", LdbcQuery1.class);
        queryMap.put("2", LdbcQuery2.class);
        queryMap.put("2a", LdbcQuery2.class);
        queryMap.put("2b", LdbcQuery2.class);
        queryMap.put("3", LdbcQuery3.class);
        queryMap.put("4", LdbcQuery4.class);
        queryMap.put("5", LdbcQuery5.class);
        queryMap.put("6", LdbcQuery6.class);
        queryMap.put("7", LdbcQuery7.class);
        queryMap.put("8", LdbcQuery8.class);
        queryMap.put("8a", LdbcQuery8.class);
        queryMap.put("8b", LdbcQuery8.class);
        queryMap.put("9", LdbcQuery9.class);
        queryMap.put("10", LdbcQuery10.class);
        queryMap.put("10a", LdbcQuery10.class);
        queryMap.put("10b", LdbcQuery10.class);
        queryMap.put("11", LdbcQuery11.class);
        queryMap.put("12", LdbcQuery12.class);
        queryMap.put("13", LdbcQuery13.class);
        queryMap.put("14", LdbcQuery14.class);
        queryMap.put("14a", LdbcQuery14.class);
        queryMap.put("14b", LdbcQuery14.class);
        queryMap.put("15", LdbcQuery15.class);
        queryMap.put("15a", LdbcQuery15.class);
        queryMap.put("15b", LdbcQuery15.class);
        queryMap.put("16", LdbcQuery16.class);
        queryMap.put("16a", LdbcQuery16.class);
        queryMap.put("16b", LdbcQuery16.class);
        queryMap.put("17", LdbcQuery17.class);
        queryMap.put("18", LdbcQuery18.class);
        queryMap.put("19", LdbcQuery19.class);
        queryMap.put("19a", LdbcQuery19.class);
        queryMap.put("19b", LdbcQuery19.class);
        queryMap.put("20", LdbcQuery20.class);
        queryMap.put("20a", LdbcQuery20.class);
        queryMap.put("20b", LdbcQuery20.class);
    }

    static Query generateOperation(List<String> params, String op_type) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method getInstanceMethod = queryMap.get(op_type).getDeclaredMethod("getInstance", List.class);
        return (Query) getInstanceMethod.invoke(null, params);
    }

    static Query deserializeOperation(String serializedOperation, String op_type) throws IOException {
        Query operation = null;
        operation = OBJECT_MAPPER.readValue(serializedOperation, queryMap.get(op_type));
        return operation;
    }

    static long ISO8601ToI64(String time) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(time, DateTimeFormatter.ISO_DATE_TIME);

        // 转换为Instant然后获取时间戳作为long
        return zonedDateTime.toInstant().toEpochMilli();
    }

    static String I64ToISO8601(long time) {
        Instant instant = Instant.ofEpochMilli(time);
        ZonedDateTime odt = instant.atZone(ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx");
       
        return formatter.format(odt);
    }

    static Date StringToDate(String date_str) {
        LocalDate localDate = LocalDate.parse(date_str);

        // 将 LocalDate 转换为系统默认时区的开始时间
        ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.of("UTC"));

        // 从 ZonedDateTime 获取 java.util.Date
        Date date = Date.from(zonedDateTime.toInstant());
        return date;
    }

    static abstract class Query<RESULT_TYPE> {
        public abstract Map<String, String> parameterMap();

        public abstract List<String> parameterList();

        public abstract List<RESULT_TYPE> deserializeResult(String serializedOperationResult) throws IOException;

        public abstract int type();

        public abstract RESULT_TYPE parseResult(List<String> value);

    }

    static class LdbcQuery1 extends Query<LdbcQuery1Result> {
        private final String datetime;

        @Override
        public LdbcQuery1Result parseResult(List<String> values) {
            values.add("0.0");
            if (values == null || values.size() < 7) {
                throw new IllegalArgumentException("The values list must contain at least 7 elements.");
            }
            return new LdbcQuery1Result(Integer.parseInt(values.get(0)), Boolean.parseBoolean(values.get(1)),
                    Integer.parseInt(values.get(2)), Long.parseLong(values.get(3)), Float.parseFloat(values.get(4)),
                    Long.parseLong(values.get(5)), Float.parseFloat(values.get(6)));
        }

        @Override
        public int type() {
            return 1;
        }


        public LdbcQuery1(
                @JsonProperty("datetime") String datetime
        ) {
            this.datetime = datetime;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery1(params.get(0));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery1 that = (LdbcQuery1) o;
            return Objects.equals(datetime, that.datetime);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(datetime);
        }

        public String getDatetime() {
            return datetime;
        }

        @Override
        public List<LdbcQuery1Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery1Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery1Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public String toString() {
            return "LdbcQuery1{" +
                    "datetime='" + datetime + '\'' +
                    '}';
        }

        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("datetime", String.valueOf(ISO8601ToI64(datetime)));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(String.valueOf(ISO8601ToI64(datetime)));
            return list;
        }
    }

    static class LdbcQuery1Result {
        private final int year;
        private final boolean isComment;
        private final int lengthCategory;
        private final long messageCount;
        private final float averageMessageLength;
        private final long sumMessageLength;
        private final float percentageOfMessages;


        public LdbcQuery1Result(
                @JsonProperty("year") int year,
                @JsonProperty("isComment") boolean isComment,
                @JsonProperty("lengthCategory") int lengthCategory,
                @JsonProperty("messageCount") long messageCount,
                @JsonProperty("averageMessageLength") float averageMessageLength,
                @JsonProperty("sumMessageLength") long sumMessageLength,
                @JsonProperty("percentageOfMessages") float percentageOfMessages
        ) {
            this.year = year;
            this.isComment = isComment;
            this.lengthCategory = lengthCategory;
            this.messageCount = messageCount;
            this.averageMessageLength = averageMessageLength;
            this.sumMessageLength = sumMessageLength;
            this.percentageOfMessages = percentageOfMessages;
        }

        public int getYear() {
            return year;
        }

        public boolean isComment() {
            return isComment;
        }

        public int getLengthCategory() {
            return lengthCategory;
        }

        public long getMessageCount() {
            return messageCount;
        }

        public float getAverageMessageLength() {
            return averageMessageLength;
        }

        public long getSumMessageLength() {
            return sumMessageLength;
        }

        public float getPercentageOfMessages() {
            return percentageOfMessages;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery1Result that = (LdbcQuery1Result) o;
            return year == that.year && isComment == that.isComment && lengthCategory == that.lengthCategory && messageCount == that.messageCount && Float.compare(averageMessageLength,that.averageMessageLength) == 0 && sumMessageLength == that.sumMessageLength && Float.compare(percentageOfMessages, that.percentageOfMessages) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(year, isComment, lengthCategory, messageCount, averageMessageLength, sumMessageLength, percentageOfMessages);
        }

        @Override
        public String toString() {
            return "LdbcQuery1Result{" +
                    "year=" + year +
                    ", isComment=" + isComment +
                    ", lengthCategory=" + lengthCategory +
                    ", messageCount=" + messageCount +
                    ", averageMessageLength=" + averageMessageLength +
                    ", sumMessageLength=" + sumMessageLength +
                    ", percentageOfMessages=" + percentageOfMessages +
                    '}';
        }
    }

    static class LdbcQuery2 extends Query<LdbcQuery2Result> {
        private final Date date;
        private final String tagClass;

        @Override
        public int type() {
            return 2;
        }

        @Override
        public LdbcQuery2Result parseResult(List<String> values) {
            System.out.println(values);
            if (values == null || values.size() < 4) {
                throw new IllegalArgumentException("The values list must contain at least 4 elements.");
            }
            return new LdbcQuery2Result(values.get(0), Integer.parseInt(values.get(1)), Integer.parseInt(values.get(2)), Integer.parseInt(values.get(3)));
        }


        public LdbcQuery2(
                @JsonProperty("date") Date date,
                @JsonProperty("tagClass") String tagClass) {
            this.date = date;
            this.tagClass = tagClass;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery2(StringToDate(params.get(0)), params.get(1));
        }

        public Date getDate() {
            return date;
        }

        public String getTagClass() {
            return tagClass;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery2 that = (LdbcQuery2) o;
            return Objects.equals(date, that.date) && Objects.equals(tagClass, that.tagClass);
        }

        @Override
        public int hashCode() {
            return Objects.hash(date, tagClass);
        }

        @Override
        public List<LdbcQuery2Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery2Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery2Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("date", String.valueOf(date.getTime()));
            map.put("tagClass", tagClass);
            map.put("dateEnd1", String.valueOf(date.getTime() + 8640000000l));
            map.put("dateEnd2", String.valueOf(date.getTime() + 2 * 8640000000l));

            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(tagClass);
            list.add(String.valueOf(date.getTime()));
            list.add(String.valueOf(date.getTime() + 8640000000l));
            list.add(String.valueOf(date.getTime() + 2 * 8640000000l));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery2{" +
                    "date=" + date +
                    ", tagClass='" + tagClass + '\'' +
                    '}';
        }
    }

    static class LdbcQuery2Result {
        private final String tag_name;
        private final int countWindow1;
        private final int countWindow2;
        private final int diff;


        public LdbcQuery2Result(
                @JsonProperty("tag.name") String tag_name,
                @JsonProperty("countWindow1") int countWindow1,
                @JsonProperty("countWindow2") int countWindow2,
                @JsonProperty("diff") int diff) {
            this.tag_name = tag_name;
            this.countWindow1 = countWindow1;
            this.countWindow2 = countWindow2;
            this.diff = diff;
        }

        public int getCountWindow2() {
            return countWindow2;
        }

        public int getCountWindow1() {
            return countWindow1;
        }

        public String getTag_name() {
            return tag_name;
        }

        public int getDiff() {
            return diff;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery2Result that = (LdbcQuery2Result) o;
            return countWindow1 == that.countWindow1 && countWindow2 == that.countWindow2 && diff == that.diff && Objects.equals(tag_name, that.tag_name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tag_name, countWindow1, countWindow2, diff);
        }

        @Override
        public String toString() {
            return "LdbcQuery2Result{" +
                    "tag_name='" + tag_name + '\'' +
                    ", countWindow1=" + countWindow1 +
                    ", countWindow2=" + countWindow2 +
                    ", diff=" + diff +
                    '}';
        }
    }

    static class LdbcQuery3 extends Query<LdbcQuery3Result> {
        private final String tagClass;
        private final String country;

        public LdbcQuery3(
                @JsonProperty("tagClass") String tagClass,
                @JsonProperty("country") String country) {
            this.tagClass = tagClass;
            this.country = country;
        }

        @Override
        public LdbcQuery3Result parseResult(List<String> values) {
            if (values == null || values.size() < 5) {
                throw new IllegalArgumentException("The values list must contain at least 5 elements.");
            }
            return new LdbcQuery3Result(Long.parseLong(values.get(0)), values.get(1), I64ToISO8601(Long.parseLong(values.get(2))), Long.parseLong(values.get(3)), Integer.parseInt(values.get(4)));
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery3(params.get(0), params.get(1));
        }

        public String getCountry() {
            return country;
        }

        public String getTagClass() {
            return tagClass;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery3 that = (LdbcQuery3) o;
            return Objects.equals(tagClass, that.tagClass) && Objects.equals(country, that.country);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tagClass, country);
        }

        @Override
        public List<LdbcQuery3Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery3Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery3Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 3;
        }

        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("tagClass", tagClass);
            map.put("country", country);
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(tagClass);
            list.add(country);
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery3{" +
                    "tagClass='" + tagClass + '\'' +
                    ", country='" + country + '\'' +
                    '}';
        }
    }

    static class LdbcQuery3Result {
        private final long forum_id;
        private final String forum_title;
        private final String forum_creationDate;
        private final long person_id;
        private final int messageCount;


        public LdbcQuery3Result(
                @JsonProperty("forum.id") long forumId,
                @JsonProperty("forum.title") String forumTitle,
                @JsonProperty("forum.creationDate") String forumCreationDate,
                @JsonProperty("person.id") long personId,
                @JsonProperty("messageCount") int messageCount) {
            forum_id = forumId;
            forum_title = forumTitle;
            this.forum_creationDate = forumCreationDate;
            person_id = personId;
            this.messageCount = messageCount;
        }

        public long getForum_id() {
            return forum_id;
        }

        public String getForum_title() {
            return forum_title;
        }

        public String getForum_creationDate() {
            return forum_creationDate;
        }

        public long getPerson_id() {
            return person_id;
        }

        public int getMessageCount() {
            return messageCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery3Result that = (LdbcQuery3Result) o;
            return forum_id == that.forum_id && forum_creationDate.equals(that.forum_creationDate) && person_id == that.person_id && messageCount == that.messageCount && Objects.equals(forum_title, that.forum_title);
        }

        @Override
        public int hashCode() {
            return Objects.hash(forum_id, forum_title, forum_creationDate, person_id, messageCount);
        }

        @Override
        public String toString() {
            return "LdbcQuery3Result{" +
                    "forum_id=" + forum_id +
                    ", forum_title='" + forum_title + '\'' +
                    ", forum_creationDate=" + forum_creationDate +
                    ", person_id=" + person_id +
                    ", messageCount=" + messageCount +
                    '}';
        }
    }

    static class LdbcQuery4 extends Query<LdbcQuery4Result> {
        private final Date date;

        public LdbcQuery4(
                @JsonProperty("date") Date date) {
            this.date = date;
        }

        @Override
        public LdbcQuery4Result parseResult(List<String> values) {
            if (values == null || values.size() < 5) {
                throw new IllegalArgumentException("The values list must contain at least 5 elements.");
            }
            return new LdbcQuery4Result(Long.parseLong(values.get(0)), values.get(1), values.get(2), I64ToISO8601(Long.parseLong(values.get(3))), Integer.parseInt(values.get(4)));
        }


        public static Query getInstance(List<String> params) {
            return new LdbcQuery4(StringToDate(params.get(0)));
        }

        public Date getDate() {
            return date;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery4 that = (LdbcQuery4) o;
            return Objects.equals(date, that.date);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(date);
        }

        @Override
        public List<LdbcQuery4Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery4Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery4Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 4;
        }

        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("date", String.valueOf(date.getTime()));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(String.valueOf(date.getTime()));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery4{" +
                    "date=" + date +
                    '}';
        }
    }

    static class LdbcQuery4Result {
        private final long person_id;
        private final String person_firstName;
        private final String person_lastName;
        private final String person_creationDate;
        private final int messageCount;


        public LdbcQuery4Result(
                @JsonProperty("person.id") long personId,
                @JsonProperty("person.firstName") String personFirstName,
                @JsonProperty("person.lastName") String personLastName,
                @JsonProperty("person.creationDate") String personCreationDate,
                @JsonProperty("messageCount") int messageCount) {
            person_id = personId;
            person_firstName = personFirstName;
            person_lastName = personLastName;
            person_creationDate = personCreationDate;
            this.messageCount = messageCount;
        }

        public long getPerson_id() {
            return person_id;
        }

        public String getPerson_firstName() {
            return person_firstName;
        }

        public String getPerson_lastName() {
            return person_lastName;
        }

        public String getPerson_creationDate() {
            return person_creationDate;
        }

        public int getMessageCount() {
            return messageCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery4Result that = (LdbcQuery4Result) o;
            return person_id == that.person_id && person_creationDate.equals(that.person_creationDate) && messageCount == that.messageCount && Objects.equals(person_firstName, that.person_firstName) && Objects.equals(person_lastName, that.person_lastName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(person_id, person_firstName, person_lastName, person_creationDate, messageCount);
        }

        @Override
        public String toString() {
            return "LdbcQuery4Result{" +
                    "person_id=" + person_id +
                    ", person_firstName='" + person_firstName + '\'' +
                    ", person_lastName='" + person_lastName + '\'' +
                    ", person_creationDate=" + person_creationDate +
                    ", messageCount=" + messageCount +
                    '}';
        }
    }


    static class LdbcQuery5 extends Query<LdbcQuery5Result> {
        private final String tag;

        @Override
        public LdbcQuery5Result parseResult(List<String> values) {
            if (values == null || values.size() < 5) {
                throw new IllegalArgumentException("The values list must contain at least 5 elements.");
            }
            return new LdbcQuery5Result(Long.parseLong(values.get(0)), Integer.parseInt(values.get(1)), Integer.parseInt(values.get(2)), Integer.parseInt(values.get(3)), Integer.parseInt(values.get(4)));
        }

        public LdbcQuery5(
                @JsonProperty("tag") String tag) {
            this.tag = tag;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery5(params.get(0));
        }

        public String getTag() {
            return tag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery5 that = (LdbcQuery5) o;
            return Objects.equals(tag, that.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tag);
        }

        @Override
        public List<LdbcQuery5Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery5Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery5Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 5;
        }

        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("tag", tag);
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(tag);
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery5{" +
                    "tag='" + tag + '\'' +
                    '}';
        }
    }

    static class LdbcQuery5Result {
        private final long person_id;
        private final int replyCount;
        private final int likeCount;
        private final int messageCount;
        private final int score;


        public LdbcQuery5Result(
                @JsonProperty("person.id") long personId,
                @JsonProperty("replyCount") int replyCount,
                @JsonProperty("likeCount") int likeCount,
                @JsonProperty("messageCount") int messageCount,
                @JsonProperty("score") int score) {
            person_id = personId;
            this.replyCount = replyCount;
            this.likeCount = likeCount;
            this.messageCount = messageCount;
            this.score = score;
        }

        public long getPerson_id() {
            return person_id;
        }

        public int getReplyCount() {
            return replyCount;
        }

        public int getLikeCount() {
            return likeCount;
        }

        public int getMessageCount() {
            return messageCount;
        }

        public int getScore() {
            return score;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery5Result that = (LdbcQuery5Result) o;
            return person_id == that.person_id && replyCount == that.replyCount && likeCount == that.likeCount && messageCount == that.messageCount && score == that.score;
        }

        @Override
        public int hashCode() {
            return Objects.hash(person_id, replyCount, likeCount, messageCount, score);
        }

        @Override
        public String toString() {
            return "LdbcQuery5Result{" +
                    "person_id=" + person_id +
                    ", replyCount=" + replyCount +
                    ", likeCount=" + likeCount +
                    ", messageCount=" + messageCount +
                    ", score=" + score +
                    '}';
        }
    }

    static class LdbcQuery6 extends Query<LdbcQuery6Result> {
        private final String tag;

        @Override
        public LdbcQuery6Result parseResult(List<String> values) {
            if (values == null || values.size() < 2) {
                throw new IllegalArgumentException("The values list must contain at least 2 elements.");
            }
            return new LdbcQuery6Result(
                    Long.parseLong(values.get(0)),
                    Integer.parseInt(values.get(1))
            );
        }

        public LdbcQuery6(
                @JsonProperty("tag") String tag) {
            this.tag = tag;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery6(params.get(0));
        }

        public String getTag() {
            return tag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery6 that = (LdbcQuery6) o;
            return Objects.equals(tag, that.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tag);
        }

        @Override
        public List<LdbcQuery6Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery6Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery6Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 6;
        }

        @Override
        public String toString() {
            return "LdbcQuery6{" +
                    "tag='" + tag + '\'' +
                    '}';
        }

        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("tag", tag);
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(tag);
            return list;
        }
    }

    static class LdbcQuery6Result {
        private final long person1_id;
        private final int authorityScore;


        public LdbcQuery6Result(
                @JsonProperty("person1.id") long person1Id,
                @JsonProperty("authorityScore") int authorityScore) {
            person1_id = person1Id;
            this.authorityScore = authorityScore;
        }

        public int getAuthorityScore() {
            return authorityScore;
        }

        public long getPerson1_id() {
            return person1_id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery6Result that = (LdbcQuery6Result) o;
            return person1_id == that.person1_id && authorityScore == that.authorityScore;
        }

        @Override
        public int hashCode() {
            return Objects.hash(person1_id, authorityScore);
        }

        @Override
        public String toString() {
            return "LdbcQuery6Result{" +
                    "person1_id=" + person1_id +
                    ", authorityScore=" + authorityScore +
                    '}';
        }
    }

    static class LdbcQuery7 extends Query<LdbcQuery7Result> {
        private final String tag;

        @Override
        public LdbcQuery7Result parseResult(List<String> values) {
            if (values == null || values.size() < 2) {
                throw new IllegalArgumentException("The values list must contain at least 2 elements.");
            }
            return new LdbcQuery7Result(
                    values.get(0), // relatedTag_name is presumed to be a String
                    Integer.parseInt(values.get(1)) // count is parsed as an Integer
            );
        }

        public LdbcQuery7(
                @JsonProperty("tag") String tag) {
            this.tag = tag;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery7(params.get(0));
        }

        public String getTag() {
            return tag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery7 that = (LdbcQuery7) o;
            return Objects.equals(tag, that.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tag);
        }

        @Override
        public List<LdbcQuery7Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery7Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery7Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 7;
        }

        @Override
        public String toString() {
            return "LdbcQuery7{" +
                    "tag='" + tag + '\'' +
                    '}';
        }

        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("tag", tag);
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(tag);
            return list;
        }
    }

    static class LdbcQuery7Result {
        private final String relatedTag_name;
        private final int count;


        public LdbcQuery7Result(
                @JsonProperty("relatedTag.name") String relatedTagName,
                @JsonProperty("count") int count) {
            relatedTag_name = relatedTagName;
            this.count = count;
        }

        public int getCount() {
            return count;
        }

        public String getRelatedTag_name() {
            return relatedTag_name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery7Result that = (LdbcQuery7Result) o;
            return count == that.count && Objects.equals(relatedTag_name, that.relatedTag_name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(relatedTag_name, count);
        }

        @Override
        public String toString() {
            return "LdbcQuery7Result{" +
                    "relatedTag_name='" + relatedTag_name + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    static class LdbcQuery8 extends Query<LdbcQuery8Result> {
        private final String tag;
        private final Date startDate;
        private final Date endDate;


        @Override
        public LdbcQuery8Result parseResult(List<String> values) {
            if (values == null || values.size() < 3) {
                throw new IllegalArgumentException("The values list must contain at least 3 elements.");
            }
            return new LdbcQuery8Result(
                    Long.parseLong(values.get(0)), // Parse person_id as Long
                    Integer.parseInt(values.get(1)), // Parse score as Integer
                    Integer.parseInt(values.get(2)) // Parse friendsScore as Integer
            );
        }

        public LdbcQuery8(
                @JsonProperty("tag") String tag,
                @JsonProperty("startDate") Date startDate,
                @JsonProperty("endDate") Date endDate) {
            this.tag = tag;
            this.startDate = startDate;
            this.endDate = endDate;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery8(params.get(0), StringToDate(params.get(1)), StringToDate(params.get(2)));
        }

        public String getTag() {
            return tag;
        }

        public Date getStartDate() {
            return startDate;
        }

        public Date getEndDate() {
            return endDate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery8 that = (LdbcQuery8) o;
            return Objects.equals(tag, that.tag) && Objects.equals(startDate, that.startDate) && Objects.equals(endDate, that.endDate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tag, startDate, endDate);
        }


        @Override
        public List<LdbcQuery8Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery8Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery8Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 8;
        }

        @Override
        public String toString() {
            return "LdbcQuery8{" +
                    "tag='" + tag + '\'' +
                    ", startDate=" + startDate +
                    ", endDate=" + endDate +
                    '}';
        }

        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("tag", tag);
            map.put("startDate", String.valueOf(startDate.getTime()));
            map.put("endDate", String.valueOf(endDate.getTime()));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(tag);
            list.add(String.valueOf(startDate.getTime()));
            list.add(String.valueOf(endDate.getTime()));
            return list;
        }
    }

    static class LdbcQuery8Result {
        private final long person_id;
        private final int score;
        private final int friendsScore;


        public LdbcQuery8Result(
                @JsonProperty("person.id") long personId,
                @JsonProperty("score") int score,
                @JsonProperty("friendsScore") int friendsScore) {
            person_id = personId;
            this.score = score;
            this.friendsScore = friendsScore;
        }

        public long getPerson_id() {
            return person_id;
        }

        public int getScore() {
            return score;
        }

        public int getFriendsScore() {
            return friendsScore;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery8Result that = (LdbcQuery8Result) o;
            return person_id == that.person_id && score == that.score && friendsScore == that.friendsScore;
        }

        @Override
        public int hashCode() {
            return Objects.hash(person_id, score, friendsScore);
        }

        @Override
        public String toString() {
            return "LdbcQuery8Result{" +
                    "person_id=" + person_id +
                    ", score=" + score +
                    ", friendsScore=" + friendsScore +
                    '}';
        }
    }

    static class LdbcQuery9 extends Query<LdbcQuery9Result> {
        private final Date startDate;
        private final Date endDate;

        public LdbcQuery9(
                @JsonProperty("startDate") Date startDate,
                @JsonProperty("endDate") Date endDate) {
            this.startDate = startDate;
            this.endDate = endDate;
        }

        @Override
        public LdbcQuery9Result parseResult(List<String> values) {
            if (values == null || values.size() < 5) {
                throw new IllegalArgumentException("The values list must contain at least 5 elements.");
            }
            return new LdbcQuery9Result(
                    Long.parseLong(values.get(0)), // person_id
                    values.get(1), // person_firstName
                    values.get(2), // person_lastName
                    Integer.parseInt(values.get(3)), // threadCount
                    Integer.parseInt(values.get(4)) // messageCount
            );
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery9(StringToDate(params.get(0)), StringToDate(params.get(1)));
        }

        public Date getStartDate() {
            return startDate;
        }

        public Date getEndDate() {
            return endDate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery9 that = (LdbcQuery9) o;
            return Objects.equals(startDate, that.startDate) && Objects.equals(endDate, that.endDate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(startDate, endDate);
        }

        @Override
        public List<LdbcQuery9Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery9Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery9Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 9;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("startDate", String.valueOf(startDate.getTime()));
            map.put("endDate", String.valueOf(endDate.getTime()));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(String.valueOf(startDate.getTime()));
            list.add(String.valueOf(endDate.getTime()));
            return list;
        }


        @Override
        public String toString() {
            return "LdbcQuery9{" +
                    "startDate=" + startDate +
                    ", endDate=" + endDate +
                    '}';
        }
    }

    static class LdbcQuery9Result {
        private final long person_id;
        private final String person_firstName;
        private final String person_lastName;
        private final int threadCount;
        private final int messageCount;


        public LdbcQuery9Result(
                @JsonProperty("person.id") long personId,
                @JsonProperty("person.firstName") String personFirstName,
                @JsonProperty("person.lastName") String personLastName,
                @JsonProperty("threadCount") int threadCount,
                @JsonProperty("messageCount") int messageCount) {
            person_id = personId;
            person_firstName = personFirstName;
            person_lastName = personLastName;
            this.threadCount = threadCount;
            this.messageCount = messageCount;
        }

        public long getPerson_id() {
            return person_id;
        }

        public String getPerson_firstName() {
            return person_firstName;
        }

        public String getPerson_lastName() {
            return person_lastName;
        }

        public int getThreadCount() {
            return threadCount;
        }

        public int getMessageCount() {
            return messageCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery9Result that = (LdbcQuery9Result) o;
            return person_id == that.person_id && threadCount == that.threadCount && messageCount == that.messageCount && Objects.equals(person_firstName, that.person_firstName) && Objects.equals(person_lastName, that.person_lastName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(person_id, person_firstName, person_lastName, threadCount, messageCount);
        }


        @Override
        public String toString() {
            return "LdbcQuery9Result{" +
                    "person_id=" + person_id +
                    ", person_firstName='" + person_firstName + '\'' +
                    ", person_lastName='" + person_lastName + '\'' +
                    ", threadCount=" + threadCount +
                    ", messageCount=" + messageCount +
                    '}';
        }
    }

    static class LdbcQuery10 extends Query<LdbcQuery10Result> {
        private final long personId;
        private final String country;
        private final String tagClass;
        private final int minPathDistance;
        private final int maxPathDistance;

        @Override
        public LdbcQuery10Result parseResult(List<String> values) {
            if (values == null || values.size() < 3) {
                throw new IllegalArgumentException("The values list must contain at least 3 elements.");
            }


            return new LdbcQuery10Result(
                    Long.parseLong(values.get(0)), // expertCandidatePerson_id
                    values.get(1), // tag_name
                    Integer.parseInt(values.get(2)) // messageCount
            );

        }


        public LdbcQuery10(
                @JsonProperty("personId") long personId,
                @JsonProperty("country") String country,
                @JsonProperty("tagClass") String tagClass,
                @JsonProperty("minPathDistance") int minPathDistance,
                @JsonProperty("maxPathDistance") int maxPathDistance) {
            this.personId = personId;
            this.country = country;
            this.tagClass = tagClass;
            this.minPathDistance = minPathDistance;
            this.maxPathDistance = maxPathDistance;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery10(Long.parseLong(params.get(0)), params.get(1), params.get(2), Integer.parseInt(params.get(3)), Integer.parseInt(params.get(4)));
        }

        public String getCountry() {
            return country;
        }

        public long getPersonId() {
            return personId;
        }

        public String getTagClass() {
            return tagClass;
        }

        public int getMinPathDistance() {
            return minPathDistance;
        }

        public int getMaxPathDistance() {
            return maxPathDistance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery10 that = (LdbcQuery10) o;
            return personId == that.personId && minPathDistance == that.minPathDistance && maxPathDistance == that.maxPathDistance && Objects.equals(country, that.country) && Objects.equals(tagClass, that.tagClass);
        }

        @Override
        public int hashCode() {
            return Objects.hash(personId, country, tagClass, minPathDistance, maxPathDistance);
        }

        @Override
        public List<LdbcQuery10Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery10Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery10Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 10;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("personId", String.valueOf(personId));
            map.put("country", country);
            map.put("tagClass", tagClass);
            map.put("minPathDistance", String.valueOf(minPathDistance));
            map.put("maxPathDistance", String.valueOf(maxPathDistance));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(String.valueOf(personId));
            list.add(country);
            list.add(tagClass);
            list.add(String.valueOf(minPathDistance));
            list.add(String.valueOf(maxPathDistance));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery10{" +
                    "personId=" + personId +
                    ", country='" + country + '\'' +
                    ", tagClass='" + tagClass + '\'' +
                    ", minPathDistance=" + minPathDistance +
                    ", maxPathDistance=" + maxPathDistance +
                    '}';
        }
    }

    static class LdbcQuery10Result {
        private final long expertCandidatePerson_id;
        private final String tag_name;
        private final int messageCount;


        public LdbcQuery10Result(
                @JsonProperty("expertCandidatePerson.id") long expertCandidatePersonId,
                @JsonProperty("tag.name") String tagName,
                @JsonProperty("messageCount") int messageCount) {
            expertCandidatePerson_id = expertCandidatePersonId;
            tag_name = tagName;
            this.messageCount = messageCount;
        }

        public long getExpertCandidatePerson_id() {
            return expertCandidatePerson_id;
        }

        public String getTag_name() {
            return tag_name;
        }

        public int getMessageCount() {
            return messageCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery10Result that = (LdbcQuery10Result) o;
            return expertCandidatePerson_id == that.expertCandidatePerson_id && messageCount == that.messageCount && Objects.equals(tag_name, that.tag_name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expertCandidatePerson_id, tag_name, messageCount);
        }

        @Override
        public String toString() {
            return "LdbcQuery10Result{" +
                    "expertCandidatePerson_id=" + expertCandidatePerson_id +
                    ", tag_name='" + tag_name + '\'' +
                    ", messageCount=" + messageCount +
                    '}';
        }
    }

    static class LdbcQuery11 extends Query<LdbcQuery11Result> {
        private final String country;
        private final Date startDate;
        private final Date endDate;

        @Override
        public LdbcQuery11Result parseResult(List<String> values) {
            if (values == null || values.isEmpty()) {
                throw new IllegalArgumentException("The values list cannot be null or empty.");
            }
            return new LdbcQuery11Result(Long.parseLong(values.get(0))); // count

        }

        public LdbcQuery11(
                @JsonProperty("country") String country,
                @JsonProperty("startDate") Date startDate,
                @JsonProperty("endDate") Date endDate) {
            this.country = country;
            this.startDate = startDate;
            this.endDate = endDate;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery11(params.get(0), StringToDate(params.get(1)), StringToDate(params.get(2)));
        }

        public String getCountry() {
            return country;
        }

        public Date getStartDate() {
            return startDate;
        }

        public Date getEndDate() {
            return endDate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery11 that = (LdbcQuery11) o;
            return Objects.equals(country, that.country) && Objects.equals(startDate, that.startDate) && Objects.equals(endDate, that.endDate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(country, startDate, endDate);
        }

        @Override
        public List<LdbcQuery11Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery11Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery11Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 11;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("country", country);
            map.put("startDate", String.valueOf(startDate.getTime()));
            map.put("endDate", String.valueOf(endDate.getTime()));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(country);
            list.add(String.valueOf(startDate.getTime()));
            list.add(String.valueOf(endDate.getTime()));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery11{" +
                    "country='" + country + '\'' +
                    ", startDate=" + startDate +
                    ", endDate=" + endDate +
                    '}';
        }
    }

    static class LdbcQuery11Result {
        private final long count;


        public LdbcQuery11Result(
                @JsonProperty("count") long count) {
            this.count = count;
        }

        public long getCount() {
            return count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery11Result that = (LdbcQuery11Result) o;
            return count == that.count;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(count);
        }

        @Override
        public String toString() {
            return "LdbcQuery11Result{" +
                    "count=" + count +
                    '}';
        }
    }

    static class LdbcQuery12 extends Query<LdbcQuery12Result> {
        private final Date startDate;
        private final int lengthThreshold;
        private final String languages;

        @Override
        public LdbcQuery12Result parseResult(List<String> values) {
            if (values == null || values.size() < 2) {
                throw new IllegalArgumentException("The values list must contain at least 2 elements.");
            }

            return new LdbcQuery12Result(
                    Integer.parseInt(values.get(0)), // messageCount
                    Integer.parseInt(values.get(1)) // personCount
            );
        }


        public LdbcQuery12(
                @JsonProperty("startDate") Date startDate,
                @JsonProperty("lengthThreshold") int lengthThreshold,
                @JsonProperty("languages") String languages) {
            this.startDate = startDate;
            this.lengthThreshold = lengthThreshold;
            this.languages = languages;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery12(StringToDate(params.get(0)), Integer.parseInt(params.get(1)), params.get(2));
        }

        public Date getStartDate() {
            return startDate;
        }

        public int getLengthThreshold() {
            return lengthThreshold;
        }

        public String getLanguages() {
            return languages;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery12 that = (LdbcQuery12) o;
            return lengthThreshold == that.lengthThreshold && Objects.equals(startDate, that.startDate) && Objects.equals(languages, that.languages);
        }

        @Override
        public int hashCode() {
            return Objects.hash(startDate, lengthThreshold, languages);
        }

        @Override
        public List<LdbcQuery12Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery12Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery12Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 12;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("startDate", String.valueOf(startDate.getTime()));
            map.put("lengthThreshold", String.valueOf(lengthThreshold));
            map.put("languages", languages);
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(String.valueOf(startDate.getTime()));
            list.add(String.valueOf(lengthThreshold));
            list.add(languages);
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery12{" +
                    "startDate=" + startDate +
                    ", lengthThreshold=" + lengthThreshold +
                    ", languages=" + languages +
                    '}';
        }
    }

    static class LdbcQuery12Result {
        private final int messageCount;
        private final int personCount;


        public LdbcQuery12Result(
                @JsonProperty("messageCount") int messageCount,
                @JsonProperty("personCount") int personCount) {
            this.messageCount = messageCount;
            this.personCount = personCount;
        }

        public int getPersonCount() {
            return personCount;
        }

        public int getMessageCount() {
            return messageCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery12Result that = (LdbcQuery12Result) o;
            return messageCount == that.messageCount && personCount == that.personCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(messageCount, personCount);
        }

        @Override
        public String toString() {
            return "LdbcQuery12Result{" +
                    "messageCount=" + messageCount +
                    ", personCount=" + personCount +
                    '}';
        }
    }

    static class LdbcQuery13 extends Query<LdbcQuery13Result> {
        private final String country;
        private final Date endDate;

        public LdbcQuery13(
                @JsonProperty("country") String country,
                @JsonProperty("endDate") Date endDate) {
            this.country = country;
            this.endDate = endDate;
        }

        @Override
        public LdbcQuery13Result parseResult(List<String> values) {
            if (values == null || values.size() < 4) {
                throw new IllegalArgumentException("The values list must contain at least 4 elements.");
            }

            return new LdbcQuery13Result(
                    Long.parseLong(values.get(0)), // zombie_id
                    Integer.parseInt(values.get(1)), // zombieLikeCount
                    Integer.parseInt(values.get(2)), // totalLikeCount
                    Float.parseFloat(values.get(3)) // zombieScore
            );

        }


        public static Query getInstance(List<String> params) {
            return new LdbcQuery13(params.get(0), StringToDate(params.get(1)));
        }

        public String getCountry() {
            return country;
        }

        public Date getEndDate() {
            return endDate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery13 that = (LdbcQuery13) o;
            return Objects.equals(country, that.country) && Objects.equals(endDate, that.endDate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(country, endDate);
        }

        @Override
        public List<LdbcQuery13Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery13Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery13Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 13;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("country", country);
            map.put("endDate", String.valueOf(endDate.getTime()));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(country);
            list.add(String.valueOf(endDate.getTime()));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery13{" +
                    "country='" + country + '\'' +
                    ", endDate=" + endDate +
                    '}';
        }
    }

    static class LdbcQuery13Result {
        private final long zombie_id;
        private final int zombieLikeCount;
        private final int totalLikeCount;
        private final float zombieScore;


        public LdbcQuery13Result(
                @JsonProperty("zombie.id") long zombieId,
                @JsonProperty("zombieLikeCount") int zombieLikeCount,
                @JsonProperty("totalLikeCount") int totalLikeCount,
                @JsonProperty("zombieScore") float zombieScore) {
            zombie_id = zombieId;
            this.zombieLikeCount = zombieLikeCount;
            this.totalLikeCount = totalLikeCount;
            this.zombieScore = zombieScore;
        }

        public long getZombie_id() {
            return zombie_id;
        }

        public int getZombieLikeCount() {
            return zombieLikeCount;
        }

        public int getTotalLikeCount() {
            return totalLikeCount;
        }

        public float getZombieScore() {
            return zombieScore;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery13Result that = (LdbcQuery13Result) o;
            return zombie_id == that.zombie_id && zombieLikeCount == that.zombieLikeCount && totalLikeCount == that.totalLikeCount && Float.compare(zombieScore, that.zombieScore) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(zombie_id, zombieLikeCount, totalLikeCount, zombieScore);
        }

        @Override
        public String toString() {
            return "LdbcQuery13Result{" +
                    "zombie_id=" + zombie_id +
                    ", zombieLikeCount=" + zombieLikeCount +
                    ", totalLikeCount=" + totalLikeCount +
                    ", zombieScore=" + zombieScore +
                    '}';
        }
    }

    static class LdbcQuery14 extends Query<LdbcQuery14Result> {
        private final String country1;
        private final String country2;

        @Override
        public LdbcQuery14Result parseResult(List<String> values) {
            if (values == null || values.size() < 4) {
                throw new IllegalArgumentException("The values list must contain at least 4 elements.");
            }

            return new LdbcQuery14Result(
                    Long.parseLong(values.get(0)), // person1_id
                    Long.parseLong(values.get(1)), // person2_id
                    values.get(2),                // city1_name
                    Integer.parseInt(values.get(3)) // score
            );

        }


        public LdbcQuery14(
                @JsonProperty("country1") String country1,
                @JsonProperty("country2") String country2) {
            this.country1 = country1;
            this.country2 = country2;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery14(params.get(0), params.get(1));
        }

        public String getCountry1() {
            return country1;
        }

        public String getCountry2() {
            return country2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery14 that = (LdbcQuery14) o;
            return Objects.equals(country1, that.country1) && Objects.equals(country2, that.country2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(country1, country2);
        }

        @Override
        public List<LdbcQuery14Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery14Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery14Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 14;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("country1", country1);
            map.put("country2", country2);
            return map;
        }


        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(country1);
            list.add(country2);
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery14{" +
                    "country1='" + country1 + '\'' +
                    ", country2='" + country2 + '\'' +
                    '}';
        }
    }

    static class LdbcQuery14Result {
        private final long person1_id;
        private final long person2_id;
        private final String city1_name;
        private final int score;


        public LdbcQuery14Result(
                @JsonProperty("person1.id") long person1Id,
                @JsonProperty("person2.id") long person2Id,
                @JsonProperty("city1.name") String city1Name,
                @JsonProperty("score") int score) {
            person1_id = person1Id;
            person2_id = person2Id;
            city1_name = city1Name;
            this.score = score;
        }

        public long getPerson1_id() {
            return person1_id;
        }

        public String getCity1_name() {
            return city1_name;
        }

        public long getPerson2_id() {
            return person2_id;
        }

        public int getScore() {
            return score;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery14Result that = (LdbcQuery14Result) o;
            return person1_id == that.person1_id && person2_id == that.person2_id && score == that.score && Objects.equals(city1_name, that.city1_name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(person1_id, person2_id, city1_name, score);
        }

        @Override
        public String toString() {
            return "LdbcQuery14Result{" +
                    "person1_id=" + person1_id +
                    ", person2_id=" + person2_id +
                    ", city1_name='" + city1_name + '\'' +
                    ", score=" + score +
                    '}';
        }
    }

    static class LdbcQuery15 extends Query<LdbcQuery15Result> {
        private final long person1Id;
        private final long person2Id;
        private final Date startDate;
        private final Date endDate;

        @Override
        public LdbcQuery15Result parseResult(List<String> values) {
            if (values == null || values.isEmpty()) {
                throw new IllegalArgumentException("The values list cannot be null or empty.");
            }

            return new LdbcQuery15Result(Float.parseFloat(values.get(0))); // weight
        }


        public LdbcQuery15(
                @JsonProperty("person1Id") long person1Id,
                @JsonProperty("person2Id") long person2Id,
                @JsonProperty("startDate") Date startDate,
                @JsonProperty("endDate") Date endDate) {
            this.person1Id = person1Id;
            this.person2Id = person2Id;
            this.startDate = startDate;
            this.endDate = endDate;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery15(Long.parseLong(params.get(0)), Long.parseLong(params.get(1)), StringToDate(params.get(2)), StringToDate(params.get(3)));
        }

        public long getPerson1Id() {
            return person1Id;
        }

        public long getPerson2Id() {
            return person2Id;
        }

        public Date getStartDate() {
            return startDate;
        }

        public Date getEndDate() {
            return endDate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery15 that = (LdbcQuery15) o;
            return person1Id == that.person1Id && person2Id == that.person2Id && Objects.equals(startDate, that.startDate) && Objects.equals(endDate, that.endDate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(person1Id, person2Id, startDate, endDate);
        }

        @Override
        public List<LdbcQuery15Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery15Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery15Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 15;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("person1Id", String.valueOf(person1Id));
            map.put("person2Id", String.valueOf(person2Id));
            map.put("startDate", String.valueOf(startDate.getTime()));
            map.put("endDate", String.valueOf(endDate.getTime()));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(String.valueOf(person1Id));
            list.add(String.valueOf(person2Id));
            list.add(String.valueOf(startDate.getTime()));
            list.add(String.valueOf(endDate.getTime()));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery15{" +
                    "person1Id=" + person1Id +
                    ", person2Id=" + person2Id +
                    ", startDate=" + startDate +
                    ", endDate=" + endDate +
                    '}';
        }
    }

    static class LdbcQuery15Result {
        private final float weight;


        public LdbcQuery15Result(
                @JsonProperty("weight") float weight) {
            this.weight = weight;
        }

        public float getWeight() {
            return weight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery15Result that = (LdbcQuery15Result) o;
            return Float.compare(weight, that.weight) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(weight);
        }

        @Override
        public String toString() {
            return "LdbcQuery15Result{" +
                    "weight=" + weight +
                    '}';
        }
    }

    static class LdbcQuery16 extends Query<LdbcQuery16Result> {
        private final String tagA;
        private final Date dateA;
        private final String tagB;
        private final Date dateB;
        private final int maxKnowsLimit;

        @Override
        public LdbcQuery16Result parseResult(List<String> values) {
            if (values == null || values.size() < 3) {
                throw new IllegalArgumentException("The values list must contain at least 3 elements.");
            }

            return new LdbcQuery16Result(
                    Long.parseLong(values.get(0)),  // person_id
                    Integer.parseInt(values.get(1)), // messageCountA
                    Integer.parseInt(values.get(2))  // messageCountB
            );
        }

        public LdbcQuery16(
                @JsonProperty("tagA") String tagA,
                @JsonProperty("dateA") Date dateA,
                @JsonProperty("tagB") String tagB,
                @JsonProperty("dateB") Date dateB,
                @JsonProperty("maxKnowsLimit") int maxKnowsLimit) {
            this.tagA = tagA;
            this.dateA = dateA;
            this.tagB = tagB;
            this.dateB = dateB;
            this.maxKnowsLimit = maxKnowsLimit;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery16(params.get(0), StringToDate(params.get(1)), params.get(2), StringToDate(params.get(3)), Integer.parseInt(params.get(4)));
        }

        public String getTagA() {
            return tagA;
        }

        public Date getDateA() {
            return dateA;
        }

        public String getTagB() {
            return tagB;
        }

        public Date getDateB() {
            return dateB;
        }

        public int getMaxKnowsLimit() {
            return maxKnowsLimit;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery16 that = (LdbcQuery16) o;
            return maxKnowsLimit == that.maxKnowsLimit && Objects.equals(tagA, that.tagA) && Objects.equals(dateA, that.dateA) && Objects.equals(tagB, that.tagB) && Objects.equals(dateB, that.dateB);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tagA, dateA, tagB, dateB, maxKnowsLimit);
        }

        @Override
        public List<LdbcQuery16Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery16Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery16Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 16;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("tagA", tagA);
            map.put("dateA", String.valueOf(dateA.getTime()));
            map.put("tagB", tagB);
            map.put("dateB", String.valueOf(dateB.getTime()));
            map.put("maxKnowsLimit", String.valueOf(maxKnowsLimit));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(tagA);
            list.add(String.valueOf(dateA.getTime()));
            list.add(tagB);
            list.add(String.valueOf(dateB.getTime()));
            list.add(String.valueOf(maxKnowsLimit));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery16{" +
                    "tagA='" + tagA + '\'' +
                    ", dateA=" + dateA +
                    ", tagB='" + tagB + '\'' +
                    ", dateB=" + dateB +
                    ", maxKnowsLimit=" + maxKnowsLimit +
                    '}';
        }
    }

    static class LdbcQuery16Result {
        private final long person_id;
        private final int messageCountA;
        private final int messageCountB;


        public LdbcQuery16Result(
                @JsonProperty("person.id") long personId,
                @JsonProperty("messageCountA") int messageCountA,
                @JsonProperty("messageCountB") int messageCountB) {
            person_id = personId;
            this.messageCountA = messageCountA;
            this.messageCountB = messageCountB;
        }

        public int getMessageCountA() {
            return messageCountA;
        }

        public long getPerson_id() {
            return person_id;
        }

        public int getMessageCountB() {
            return messageCountB;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery16Result that = (LdbcQuery16Result) o;
            return person_id == that.person_id && messageCountA == that.messageCountA && messageCountB == that.messageCountB;
        }

        @Override
        public int hashCode() {
            return Objects.hash(person_id, messageCountA, messageCountB);
        }

        @Override
        public String toString() {
            return "LdbcQuery16Result{" +
                    "person_id=" + person_id +
                    ", messageCountA=" + messageCountA +
                    ", messageCountB=" + messageCountB +
                    '}';
        }
    }

    static class LdbcQuery17 extends Query<LdbcQuery17Result> {
        private final String tag;
        private final int delta;

        @Override
        public LdbcQuery17Result parseResult(List<String> values) {
            if (values == null || values.size() < 2) {
                throw new IllegalArgumentException("The values list must contain at least 2 elements.");
            }

            return new LdbcQuery17Result(
                    Long.parseLong(values.get(0)),  // person1_id
                    Integer.parseInt(values.get(1)) // messageCount
            );
        }


        public LdbcQuery17(
                @JsonProperty("tag") String tag,
                @JsonProperty("delta") int delta) {
            this.tag = tag;
            this.delta = delta;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery17(params.get(0), Integer.parseInt(params.get(1)));
        }

        public int getDelta() {
            return delta;
        }

        public String getTag() {
            return tag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery17 that = (LdbcQuery17) o;
            return delta == that.delta && Objects.equals(tag, that.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tag, delta);
        }

        @Override
        public List<LdbcQuery17Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery17Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery17Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 17;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("tag", tag);
            map.put("delta", String.valueOf(delta));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(tag);
            list.add(String.valueOf(delta));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery17{" +
                    "tag='" + tag + '\'' +
                    ", delta=" + delta +
                    '}';
        }
    }

    static class LdbcQuery17Result {
        private final long person1_id;
        private final int messageCount;


        public LdbcQuery17Result(
                @JsonProperty("person1.id") long person1Id,
                @JsonProperty("messageCount") int messageCount) {
            person1_id = person1Id;
            this.messageCount = messageCount;
        }

        public long getPerson1_id() {
            return person1_id;
        }

        public int getMessageCount() {
            return messageCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery17Result that = (LdbcQuery17Result) o;
            return person1_id == that.person1_id && messageCount == that.messageCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(person1_id, messageCount);
        }

        @Override
        public String toString() {
            return "LdbcQuery17Result{" +
                    "person1_id=" + person1_id +
                    ", messageCount=" + messageCount +
                    '}';
        }
    }

    static class LdbcQuery18 extends Query<LdbcQuery18Result> {
        private final String tag;

        @Override
        public LdbcQuery18Result parseResult(List<String> values) {
            if (values == null || values.size() < 3) {
                throw new IllegalArgumentException("The values list must contain at least 3 elements.");
            }

            return new LdbcQuery18Result(
                    Long.parseLong(values.get(0)),  // person1_id
                    Long.parseLong(values.get(1)),  // person2_id
                    Integer.parseInt(values.get(2)) // mutualFriendCount
            );
        }


        public LdbcQuery18(
                @JsonProperty("tag") String tag) {
            this.tag = tag;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery18(params.get(0));
        }

        public String getTag() {
            return tag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery18 that = (LdbcQuery18) o;
            return Objects.equals(tag, that.tag);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tag);
        }

        @Override
        public List<LdbcQuery18Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery18Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery18Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 18;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("tag", tag);
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(tag);
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery18{" +
                    "tag='" + tag + '\'' +
                    '}';
        }
    }

    static class LdbcQuery18Result {
        private final long person1_id;
        private final long person2_id;
        private final int mutualFriendCount;


        public LdbcQuery18Result(
                @JsonProperty("person1.id") long person1Id,
                @JsonProperty("person2.id") long person2Id,
                @JsonProperty("mutualFriendCount") int mutualFriendCount) {
            person1_id = person1Id;
            person2_id = person2Id;
            this.mutualFriendCount = mutualFriendCount;
        }

        public long getPerson1_id() {
            return person1_id;
        }

        public long getPerson2_id() {
            return person2_id;
        }

        public int getMutualFriendCount() {
            return mutualFriendCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery18Result that = (LdbcQuery18Result) o;
            return person1_id == that.person1_id && person2_id == that.person2_id && mutualFriendCount == that.mutualFriendCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(person1_id, person2_id, mutualFriendCount);
        }

        @Override
        public String toString() {
            return "LdbcQuery18Result{" +
                    "person1_id=" + person1_id +
                    ", person2_id=" + person2_id +
                    ", mutualFriendCount=" + mutualFriendCount +
                    '}';
        }
    }

    static class LdbcQuery19 extends Query<LdbcQuery19Result> {
        private final long city1Id;
        private final long city2Id;

        @Override
        public LdbcQuery19Result parseResult(List<String> values) {
            if (values == null || values.size() < 3) {
                throw new IllegalArgumentException("The values list must contain at least 3 elements.");
            }

            return new LdbcQuery19Result(
                    Long.parseLong(values.get(0)),  // person1_id
                    Long.parseLong(values.get(1)),  // person2_id
                    Integer.parseInt(values.get(2)) // totalWeight
            );
        }


        public LdbcQuery19(
                @JsonProperty("city1Id") long city1Id,
                @JsonProperty("city2Id") long city2Id) {
            this.city1Id = city1Id;
            this.city2Id = city2Id;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery19(Long.parseLong(params.get(0)), Long.parseLong(params.get(1)));
        }

        public long getCity1Id() {
            return city1Id;
        }

        public long getCity2Id() {
            return city2Id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery19 that = (LdbcQuery19) o;
            return city1Id == that.city1Id && city2Id == that.city2Id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(city1Id, city2Id);
        }

        @Override
        public List<LdbcQuery19Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery19Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery19Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 19;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("city1Id", String.valueOf(city1Id));
            map.put("city2Id", String.valueOf(city2Id));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(String.valueOf(city1Id));
            list.add(String.valueOf(city2Id));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery19{" +
                    "city1Id=" + city1Id +
                    ", city2Id=" + city2Id +
                    '}';
        }
    }

    static class LdbcQuery19Result {
        private final long person1_id;
        private final long person2_id;
        private final int totalWeight;


        public LdbcQuery19Result(
                @JsonProperty("person1.id") long person1Id,
                @JsonProperty("person2.id") long person2Id,
                @JsonProperty("totalWeight") int totalWeight) {
            person1_id = person1Id;
            person2_id = person2Id;
            this.totalWeight = totalWeight;
        }

        public long getPerson1_id() {
            return person1_id;
        }

        public long getPerson2_id() {
            return person2_id;
        }

        public int getTotalWeight() {
            return totalWeight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery19Result that = (LdbcQuery19Result) o;
            return person1_id == that.person1_id && person2_id == that.person2_id && totalWeight == that.totalWeight;
        }

        @Override
        public int hashCode() {
            return Objects.hash(person1_id, person2_id, totalWeight);
        }

        @Override
        public String toString() {
            return "LdbcQuery19Result{" +
                    "person1_id=" + person1_id +
                    ", person2_id=" + person2_id +
                    ", totalWeight=" + totalWeight +
                    '}';
        }
    }

    static class LdbcQuery20 extends Query<LdbcQuery20Result> {
        private final String company;
        private final long person2Id;

        @Override
        public LdbcQuery20Result parseResult(List<String> values) {
            if (values == null || values.size() < 2) {
                throw new IllegalArgumentException("The values list must contain at least 2 elements.");
            }

            return new LdbcQuery20Result(
                    Long.parseLong(values.get(0)),  // person1_id
                    Integer.parseInt(values.get(1)) // totalWeight
            );
        }


        public LdbcQuery20(
                @JsonProperty("company") String company,
                @JsonProperty("person2Id") long person2Id) {
            this.company = company;
            this.person2Id = person2Id;
        }

        public static Query getInstance(List<String> params) {
            return new LdbcQuery20(params.get(0), Long.parseLong(params.get(1)));
        }

        public String getCompany() {
            return company;
        }

        public long getPerson2Id() {
            return person2Id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery20 that = (LdbcQuery20) o;
            return person2Id == that.person2Id && Objects.equals(company, that.company);
        }

        @Override
        public int hashCode() {
            return Objects.hash(company, person2Id);
        }


        @Override
        public List<LdbcQuery20Result> deserializeResult(String serializedResults) throws IOException {
            List<LdbcQuery20Result> marshaledOperationResult;
            marshaledOperationResult = Arrays.asList(OBJECT_MAPPER.readValue(serializedResults, LdbcQuery20Result[].class));
            return marshaledOperationResult;
        }

        @Override
        public int type() {
            return 20;
        }


        @Override
        public Map<String, String> parameterMap() {
            Map<String, String> map = new HashMap<>();
            map.put("company", company);
            map.put("person2Id", String.valueOf(person2Id));
            return map;
        }

        @Override
        public List<String> parameterList() {
            List<String> list = new ArrayList<>();
            list.add(company);
            list.add(String.valueOf(person2Id));
            return list;
        }

        @Override
        public String toString() {
            return "LdbcQuery20{" +
                    "company='" + company + '\'' +
                    ", person2Id=" + person2Id +
                    '}';
        }
    }

    static class LdbcQuery20Result {
        private final long person1_id;
        private final int totalWeight;


        public LdbcQuery20Result(
                @JsonProperty("person1.id") long person1Id,
                @JsonProperty("totalWeight") int totalWeight) {
            person1_id = person1Id;
            this.totalWeight = totalWeight;
        }

        public long getPerson1_id() {
            return person1_id;
        }

        public int getTotalWeight() {
            return totalWeight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LdbcQuery20Result that = (LdbcQuery20Result) o;
            return person1_id == that.person1_id && totalWeight == that.totalWeight;
        }

        @Override
        public int hashCode() {
            return Objects.hash(person1_id, totalWeight);
        }

        @Override
        public String toString() {
            return "LdbcQuery20Result{" +
                    "person1_id=" + person1_id +
                    ", totalWeight=" + totalWeight +
                    '}';
        }
    }
}

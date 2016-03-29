package org.mazerunner.core.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Copyright (C) 2014 Kenny Bastani
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
public class ConfigurationLoader {

    public static boolean testPropertyAccess = false;

    public static final String HADOOP_CORE_SITE_KEY = "org.mazerunner.hadoop.core.path";
    public static final String HADOOP_HDFS_SITE_KEY = "org.mazerunner.hadoop.hdfs.path";
    public static final String HADOOP_HDFS_URI = "org.mazerunner.hadoop.hdfs.uri";
    public static final String MAZERUNNER_RELATIONSHIP_TYPE_KEY = "org.mazerunner.job.relationshiptype";
    public static final String RABBITMQ_NODENAME_KEY = "org.mazerunner.rabbitmq.nodename";
    public static final String SPARK_HOST = "org.mazerunner.spark.host";
    private String hadoopSitePath;
    private String hadoopHdfsPath;
    private String hadoopHdfsUri;
    private String mazerunnerRelationshipType;

    public void setRabbitmqNodename(String rabbitmqNodename) {
        this.rabbitmqNodename = rabbitmqNodename;
    }

    private String rabbitmqNodename;

    public String getDriverHost() {
        return driverHost;
    }

    public void setDriverHost(String driverHost) {
        this.driverHost = driverHost;
    }

    private String driverHost;

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }


    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    private String executorMemory;
    private String appName;

    private String sparkHost;

    //Modify March2
    private String driverMemory;
    private String executorInstances;
    private String memoryFraction;
    private String driverExtraJava;
    private String executorExtraJava;
    private String sparkSerializer;
    private String registRequired;
    private String kryoBuffer;
    private String rddCompress;
    private String taskMaxFailure;
    private String akkaFrameSize;

    public void setDriverMemory(String driverMemory){
        this.driverMemory = driverMemory;
    }
    public String getDriverMemory(){
        return driverMemory;
    }

    public void setExecutorInstances(String executorInstances){
        this.executorInstances = executorInstances;
    }
    public String getExecutorInstances(){
        return executorInstances;
    }

    public void setMemoryFraction(String memoryFraction){
        this.memoryFraction = memoryFraction;
    }
    public String getMemoryFraction(){
        return memoryFraction;
    }

    public void setDriverExtraJava(String driverExtraJava){
        this.driverExtraJava = driverExtraJava;
    }
    public String getDriverExtraJava(){
        return driverExtraJava;
    }

    public void setExecutorExtraJava(String executorExtraJava){
        this.executorExtraJava = executorExtraJava;
    }
    public String getExecutorExtraJava(){
        return executorExtraJava;
    }

    public void setSparkSerializer(String sparkSerializer){
        this.sparkSerializer = sparkSerializer;
    }
    public String getSparkSerializer(){
        return sparkSerializer;
    }

    public void setRegistRequired(String registRequired){
        this.registRequired = registRequired;
    }
    public String getRegistRequired(){
        return registRequired;
    }

    public void setKryoBuffer(String kryoBuffer){
        this.kryoBuffer = kryoBuffer;
    }
    public String getKryoBuffer(){
        return kryoBuffer;
    }

    public void setRddCompress(String rddCompress){
        this.rddCompress = rddCompress;
    }
    public String getRddCompress(){
        return rddCompress;
    }

    public void setTaskMaxFailure(String taskMaxFailure){
        this.taskMaxFailure = taskMaxFailure;
    }
    public String getTaskMaxFailure(){
        return taskMaxFailure;
    }

    public void setAkkaFrameSize(String akkaFrameSize){
        this.akkaFrameSize = akkaFrameSize;
    }
    public String getAkkaFrameSize(){
        return akkaFrameSize;
    }



    //Modify March2


    private static ConfigurationLoader instance = null;

    protected ConfigurationLoader() {

    }

    public static ConfigurationLoader getInstance() {
        if(instance == null)
        {
            instance = new ConfigurationLoader();
            try {
                if(!testPropertyAccess) {
                    instance.initialize();
                } else {
                    instance.initializeTest();
                }
            }
            catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return instance;
    }

    public void initialize() throws IOException {
        Properties prop = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream stream = loader.getResourceAsStream("spark.properties");
        prop.load(stream);

        hadoopSitePath = prop.getProperty(HADOOP_CORE_SITE_KEY);
        hadoopHdfsPath = prop.getProperty(HADOOP_HDFS_SITE_KEY);
        mazerunnerRelationshipType = prop.getProperty(MAZERUNNER_RELATIONSHIP_TYPE_KEY);
        rabbitmqNodename = prop.getProperty(RABBITMQ_NODENAME_KEY);
    }

    public void initializeTest()
    {
        hadoopSitePath = "/hadoop-2.4.1/conf/core-site.xml";
        hadoopHdfsPath = "/hadoop-2.4.1/conf/hdfs-site.xml";
        hadoopHdfsUri = "hdfs://0.0.0.0:9000";
        mazerunnerRelationshipType = "CONNECTED_TO";
        rabbitmqNodename = "localhost";
        sparkHost = "local";
        appName = "mazerunner";
        executorMemory = "2g";

        //Modify March2
        driverMemory = "4g";
        executorInstances = "10";
        memoryFraction = "0.6";
    }

    public String getMazerunnerRelationshipType() {
        return mazerunnerRelationshipType;
    }

    public String getHadoopSitePath() {
        return hadoopSitePath;
    }

    public String getHadoopHdfsPath() {
        return hadoopHdfsPath;
    }

    public String getHadoopHdfsUri() {
        return hadoopHdfsUri;
    }

    public String getRabbitmqNodename() {
        return rabbitmqNodename;
    }

    public String getSparkHost() {
        return sparkHost;
    }


    public void setSparkHost(String sparkHost) {
        this.sparkHost = sparkHost;
    }

    public void setHadoopHdfsUri(String hadoopHdfsUri) {
        this.hadoopHdfsUri = hadoopHdfsUri;
    }
}

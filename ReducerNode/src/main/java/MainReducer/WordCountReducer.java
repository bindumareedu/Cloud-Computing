package MainReducer;

import com.keyval_grpc.internalFiles.KeyValueGrpc;
import com.keyval_grpc.internalFiles.Keyval;
import com.mapreduce.grpc.internalFiles.Mapreduce;
import com.mapreduce.grpc.internalFiles.MasterGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.logging.*;

public class WordCountReducer {
    private static String reducerSet = "abcdefghijklmnopqrstuvwxyz";
    private static String[] inputmapperDirectories;
    private static String outputDirectory = "";
    private static String inputFileLocation = "";
    private static ArrayList<String> words = new ArrayList<>();
    private static HashSet<String> keySet = new HashSet<>();
    private static KeyValueGrpc.KeyValueBlockingStub stub;
    static final Logger LOGGER = Logger.getLogger(WordCountReducer.class.getName());
    static Handler consoleHandler = null;
    static Handler fileHandler = null;
    static SimpleFormatter simpleFormatter = null;
    static Properties jvm = System.getProperties();
    static String BUCKET_NAME ="mapreduce-storage";
    static String inputFileBucketPath="";



    public static void initLogger() {
        try{
            consoleHandler = new ConsoleHandler();
            fileHandler = new FileHandler(jvm.getProperty("user.dir")+"/Logger.log",true);
            simpleFormatter = new SimpleFormatter();

            LOGGER.addHandler(consoleHandler);
            LOGGER.addHandler(fileHandler);

            consoleHandler.setLevel(Level.ALL);
            fileHandler.setLevel(Level.ALL);
            fileHandler.setFormatter(simpleFormatter);
            LOGGER.setLevel(Level.ALL);
            LOGGER.config("Logger configuration done");

        }catch(IOException e){
            LOGGER.log(Level.SEVERE, "Exception: ",e);
        }

    }


    private static void reduceOperation(){
        FileInputStream fis;
        BufferedReader reader;
        System.out.println(outputDirectory);
        for(String directory : inputmapperDirectories){
            for(String word : words){
                String s = "";
                s += word.charAt(0);
                if(reducerSet.contains(s)) {
                    String currentFileName = "/"+word + ".txt";
                    File fileinCurrentDirectory = new File(directory+currentFileName);
//                    Path currentDirectoryPath = java.nio.file.Paths.get(tmpdirpath, mainOutputDirectory, directory, currentFileName);
//                    System.out.println("out dir exists?" + currentDirectoryPath.toString());
                    String requestmsg = "GET " + word + "\n";
                    String updatedRequestValue = "";
                    Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(directory).build();
                    String responsemsg = stub.serverOperation(request).getOutput();
                    String[] msgs = responsemsg.split("\n");
                    // System.out.println("response   " + responsemsg);
                    if (!responsemsg.trim().equals("UNSUCCESSFUL")) {
                        responsemsg = msgs[1].trim();
                        addKeyValueToReducerDirectory(word,responsemsg);
                    }

                    /*
                    if (fileinCurrentDirectory.exists()) {
                        try {
                            fis = new FileInputStream(fileinCurrentDirectory);
                            reader = new BufferedReader(new InputStreamReader(fis));
                            String count = reader.readLine();
                            addKeyValueToReducerDirectory(word,count);
                        } catch (Exception e) {
                            LOGGER.log(Level.SEVERE, "Exception: ",e);
                        }
                    }*/
                }
            }
        }
    }


    private static void addKeyValueToReducerDirectory(String currrentWord,String value){
        System.out.println(outputDirectory +" i add key reducen");
        String responsemsg = "";
        String newresponse = "";
        if (keySet.contains(currrentWord)) {
            //System.out.println(s+" contained");
            String requestmsg = "GET " + currrentWord + "\n";
            String updatedRequestValue = "";
            Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(outputDirectory).build();
            responsemsg = stub.serverOperation(request).getOutput();
            String[] msgs = responsemsg.split("\n");
            // System.out.println("response   " + responsemsg);
            if (!responsemsg.trim().equals("UNSUCCESSFUL")) {
                responsemsg = msgs[1].trim();
                //System.out.println("response msg " + responsemsg );
                int newval = Integer.parseInt(responsemsg) + Integer.parseInt(value);
                updatedRequestValue += newval;
                // System.out.println("updated val "+updatedRequestValue);
                int noOfBytes = updatedRequestValue.getBytes().length;
                String num = Integer.toString(noOfBytes);
                requestmsg = "SET " + currrentWord + " " + num + "\r\n" + updatedRequestValue + "\r\n";
                request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(outputDirectory).build();
                newresponse = stub.serverOperation(request).getOutput();
                // System.out.println("response "+newresponse);
            }
        } else {
            //System.out.println("new s " +s);
            keySet.add(currrentWord);
            String requestValue = value;
            int noOfBytes = requestValue.getBytes().length;
            String num = Integer.toString(noOfBytes);
            String requestmsg = "SET " + currrentWord + " " + num + "\r\n" + requestValue + "\r\n";
            Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(outputDirectory).build();
            String newResponse = stub.serverOperation(request).getOutput();
        }
    }

    private static ArrayList<String> createList(String inputFileLocation){
        ArrayList<String> words = new ArrayList<>();
        File inputFile = new File(inputFileLocation);
        FileInputStream fis;
        BufferedReader reader;
        try {
            fis = new FileInputStream(inputFile);
            reader = new BufferedReader(new InputStreamReader(fis));
            String word;
            while((word = reader.readLine()) != null){
                words.add(word);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception: ",e);
        }
        return words;
    }

    public static boolean isPathValid(String path){
        File f = new File(path);
        return (f.exists());
    }

    public static String checkarguments(String[] args){
        if(!args[0].equals("-i") || !args[2].equals("-d") || !args[4].equals("-o")){
            return "Arguments are invalid";
        }
        inputFileBucketPath = args[1];
        String dirlists = args[3];
        outputDirectory = args[5];

        inputmapperDirectories = dirlists.split(",");

//        if(!isPathValid(inputFileLocation)){
//            return "Input file is not valid";
//        }
//
//        for (String dir : inputmapperDirectories) {
//            if(!isPathValid(dir)){
//                return dir + " Output directory path is invalid";
//            }
//        }
        LOGGER.info("Arguments are accepted");
        return "none";

    }

    private static void downloadFileFromBucket(String inputFileBucketPath) {
        List<String> argumentList = new ArrayList<>();
        argumentList.add("gsutil");
        argumentList.add("cp");
        argumentList.add("gs://"+BUCKET_NAME+inputFileBucketPath);
        argumentList.add(".");
        //  LOGGER.info("Calling mapper function with command: "+argumentList);
        try {
            String output = new ProcessExecutor().command(argumentList)
                    .readOutput(true).execute()
                    .outputUTF8();
            System.out.println(output);


        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while launching reducer process",e);

        }
        int index = inputFileBucketPath.lastIndexOf('/');
        String newIntermediateName = inputFileBucketPath.substring(index+1);
        inputFileLocation = System.getProperty("user.dir")+"/"+newIntermediateName;
    }


    private static void createOutputDirectory(){
        File od = new File(outputDirectory);
        if(!od.exists()) od.mkdirs();
        od.setWritable(true);
        od.setReadable(true);
        od.setExecutable(true);
    }

    public static void main(String[] arg){
        initLogger();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("111.106.185.35.bc.googleusercontent.com",1993).usePlaintext().build();
        stub = KeyValueGrpc.newBlockingStub(channel);
        if(arg.length!=6){
            System.out.println("Please give all arguments");
            return;
        }
        String check = checkarguments(arg);
        if(!check.equals("none")){
            System.out.println(check);
            return;
        }
        System.out.println("Everything is fine");
        //LOGGER.info("Creating the directory to save reducer output");
        //LOGGER.info("Need to download input fro bucket");
        downloadFileFromBucket(inputFileBucketPath);
        LOGGER.info("Downloaded input from bucket");
        // createOutputDirectory();
        LOGGER.info("Parsing input file and loading it to internal data structure");
        words=createList(inputFileLocation);
        System.out.println("len "+words.size());
        LOGGER.info("Calling the reduce operation");
        reduceOperation();
        LOGGER.info("Completed the reduce operation");
        updateStatusToMaster();

    }
    private static void updateStatusToMaster() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("217.222.73.34.bc.googleusercontent.com",1992).usePlaintext().build();
        MasterGrpc.MasterBlockingStub stub = MasterGrpc.newBlockingStub(channel);
        Mapreduce.Request request = Mapreduce.Request.newBuilder().setStatus(1).build();
        stub.masterNode(request);
    }


    private static String downloadFinalOutput(String inputFileBucketPath) {
        List<String> argumentList = new ArrayList<>();
        argumentList.add("gsutil");
        argumentList.add("cp");
        argumentList.add("gs://"+BUCKET_NAME+inputFileBucketPath);
        argumentList.add(".");
        //  LOGGER.info("Calling mapper function with command: "+argumentList);
        try {
            String output = new ProcessExecutor().command(argumentList)
                    .readOutput(true).execute()
                    .outputUTF8();
            System.out.println(output);


        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while launching reducer process",e);

        }
        int index = inputFileBucketPath.lastIndexOf('/');
        String newIntermediateName = inputFileBucketPath.substring(index+1);
        return System.getProperty("user.dir")+"/"+newIntermediateName;
    }

//    public static void combinedToOneFile(){
//        createOutputDirectory();
//        String path = System.getProperty(outputDirectory);
//        String outputFileWithAllWords = path+"/allwords_output.txt";
//        String finalAllWords = downloadFinalOutput(outputFileWithAllWords);
//        List<String> in = new ArrayList<>();
//
//        outputFileWithAllWords = writeToFile(in, outputFileWithAllWords);
//        uploadFileIncludingPath(outputFileWithAllWords);
//    }

}

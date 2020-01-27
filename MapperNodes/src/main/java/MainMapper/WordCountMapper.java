package MainMapper;

import com.keyval_grpc.internalFiles.KeyValueGrpc;
import com.keyval_grpc.internalFiles.Keyval;
import com.mapreduce.grpc.internalFiles.Mapreduce;
import com.mapreduce.grpc.internalFiles.MasterGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.logging.*;

public class WordCountMapper {
    private static ArrayList<String> words = new ArrayList<>();
    private static String outputDirectory = "";
    private static String inputFile;
    private static String inputFileBucketPath;
    static final Logger LOGGER = Logger.getLogger(WordCountMapper.class.getName());
    static Handler consoleHandler = null;
    static Handler fileHandler = null;
    static SimpleFormatter simpleFormatter = null;
    static Properties jvm = System.getProperties();
    static String BUCKET_NAME ="mapreduce-storage";



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

        }catch(IOException exception){
            LOGGER.log(Level.SEVERE, "Error in FileHandler",exception);
        }

    }



    public static void updateKeyVal(KeyValueGrpc.KeyValueBlockingStub stub) {
        System.out.println("in mapper" +outputDirectory);
        String responsemsg = "";
        String newresponse = "";
        HashSet<String> set = new HashSet();
        for (String s : words) {
            int len = s.length();
            if(len > 1)
            {
                if (set.contains(s)) {
                    //System.out.println(s+" contained");
                    String requestmsg = "GET " + s + "\n";
                    String updatedRequestValue = "";
                    Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(outputDirectory).build();
                    responsemsg = stub.serverOperation(request).getOutput();
                    String[] msgs = responsemsg.split("\n");
               // System.out.println("response   " + responsemsg);
                    if (!responsemsg.trim().equals("UNSUCCESSFUL")) {
                        responsemsg = msgs[1].trim();
                        //System.out.println("response msg " + responsemsg );
                        int newval = Integer.parseInt(responsemsg) + 1;
                        updatedRequestValue += newval;
                       // System.out.println("updated val "+updatedRequestValue);
                        int noOfBytes = updatedRequestValue.getBytes().length;
                        String num = Integer.toString(noOfBytes);
                        requestmsg = "SET " + s + " " + num + "\r\n" + updatedRequestValue + "\r\n";
                        //System.out.println(requestmsg);
                        request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(outputDirectory).build();
                        newresponse = stub.serverOperation(request).getOutput();
                       // System.out.println("response "+newresponse);
                    }
                } else {
                    //System.out.println("new s " +s);
                    set.add(s);
                    String requestValue = "1";
                    int noOfBytes = requestValue.getBytes().length;
                    String num = Integer.toString(noOfBytes);
                    String requestmsg = "SET " + s + " " + num + "\r\n" + requestValue + "\r\n";
                    Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(outputDirectory).build();
                    String newResponse = stub.serverOperation(request).getOutput();
                }
            }
        }
    }

    public static void createList(String inputFileString){
        //java.nio.file.Path inputFilePath = java.nio.file.Paths.get(tmpdirpath,outputMainFolder,inputFileString);
        File inputFile = new File(inputFileString);
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
    }


    private static void downloadFileFromBucket(String inputFileBucketPath) {
        LOGGER.config("Before downloading input from bucket");

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
            LOGGER.log(Level.SEVERE, "Error while launching mapper process",e);

        }
        int index = inputFileBucketPath.lastIndexOf('/');
        String newIntermediateName = inputFileBucketPath.substring(index+1);
        inputFile = System.getProperty("user.dir")+"/"+newIntermediateName;
        LOGGER.config("Successfully downloaded from bucket");

    }
    /*
    public static void createOutputFolder(){
        if(!Files.exists(Paths.get(outputDirectory))) {
            try {
                Files.createDirectories(Paths.get(outputDirectory));
            } catch (IOException e) {
                //add comments to logger
                LOGGER.log(Level.SEVERE, "Exception: ",e);
            }
        }
    }
    */

    public static boolean isPathValid(String path){
        File f = new File(path);
        return (f.exists());
    }

    public static String checkarguments(String[] args){
        if(!args[0].equals("-i") || !args[2].equals("-o")){
            return "Arguments are invalid";
        }
        inputFileBucketPath = args[1];
        outputDirectory = args[3];
        //Call function to download from BUCKET
//        if(!isPathValid(inputFile)){
//            return "Input path not valid";
//        }

//        if(!isPathValid(outputDirectory)){
//            return "Output Directory not valid";
//        }
        LOGGER.info("Arguments are accepted");

        return "none";


    }

    public static void main(String[] arg){
        initLogger();
        String check = checkarguments(arg);
        if(!check.equals("none")){
            System.out.println(check);
            return;
        }
        //LOGGER.info("Creating the directory to save mapper output");
       // createOutputFolder();
        System.out.println("Everything is fine");
        downloadFileFromBucket(inputFileBucketPath);
        LOGGER.info("Parsing input file and loading it to internal data structure");
        createList(inputFile);
        LOGGER.info("Opening channel");
        ManagedChannel channel = ManagedChannelBuilder.forAddress("111.106.185.35.bc.googleusercontent.com",1993).usePlaintext().build();
        LOGGER.info("Creating RPC Stub");
        KeyValueGrpc.KeyValueBlockingStub stub = KeyValueGrpc.newBlockingStub(channel);
        LOGGER.info("Calling the mapper function");
        updateKeyVal(stub);
        LOGGER.info("Completed the mapper function");
        updateStatusToMaster();

    }

    private static void updateStatusToMaster() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("217.222.73.34.bc.googleusercontent.com",1992).usePlaintext().build();
        MasterGrpc.MasterBlockingStub stub = MasterGrpc.newBlockingStub(channel);
        Mapreduce.Request request = Mapreduce.Request.newBuilder().setStatus(1).build();
        stub.masterNode(request);
    }


}

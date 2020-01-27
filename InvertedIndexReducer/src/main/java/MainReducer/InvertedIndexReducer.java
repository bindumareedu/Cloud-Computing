package MainReducer;

import com.keyval_grpc.internalFiles.KeyValueGrpc;
import com.keyval_grpc.internalFiles.Keyval;
import com.mapreduce.grpc.internalFiles.Mapreduce;
import com.mapreduce.grpc.internalFiles.MasterGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.*;

//there is problem with  addKeyValueToReducerDirectory function

public class InvertedIndexReducer {
    private static String reducerSet = "abcdefghijklmnopqrstuvwxyz";
    private static String[] inputmapperDirectories;
    private static String outputDirectory = "";
    private static String inputFileLocation = "";
    private static ArrayList<String> words = new ArrayList<>();
    private static Set<String> keySet = Collections.synchronizedSet(new HashSet());
    private static KeyValueGrpc.KeyValueBlockingStub stub;
    private static Properties directoryToInputFileProperties = new Properties();

    private static String directoryToInputFilePath = "";
    private static HashMap<String,String> propertiesFileHashMap = new HashMap<>();
    //= java.nio.file.Paths.get(tmpdirpath,"Asst2",outputDirectory);
    static final Logger LOGGER = Logger.getLogger(InvertedIndexReducer.class.getName());
    static Handler consoleHandler = null;
    static Handler fileHandler = null;
    static SimpleFormatter simpleFormatter = null;
    static Properties jvm = System.getProperties();
    static String BUCKET_NAME ="mapreduce-storage";
    static String inputFileBucketPath="";
    static String inputPropertiesFileBucketPath = "";


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


    private static void reduceOperation(){
        FileInputStream fis;
        BufferedReader reader;
        for(String word : words){
           // System.out.println(word);
            for(String directory : inputmapperDirectories){
                String currentFileName = "/"+word + ".txt";
                    File fileinCurrentDirectory = new File(directory+currentFileName);
                String requestmsg = "GET " + word + "\n";
                String updatedRequestValue = "";
                Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(directory).build();
                String responsemsg = stub.serverOperation(request).getOutput();
                String[] msgs = responsemsg.split("\n");
                // System.out.println("response   " + responsemsg);
                if (!responsemsg.trim().equals("UNSUCCESSFUL")) {
                    responsemsg = msgs[1].trim();
                    addKeyValueToReducerDirectory(word,directory,responsemsg);
                }


//                if (fileinCurrentDirectory.exists()) {
//                       // System.out.println("out dir exists?" + currentDirectoryPath.toString());
//                        try {
//                            fis = new FileInputStream(fileinCurrentDirectory);
//                            reader = new BufferedReader(new InputStreamReader(fis));
//                            String value = reader.readLine();
//                            addKeyValueToReducerDirectory(word,directory,value);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
                //}
            }
        }
    }

    private static void addKeyValueToReducerDirectory(String currrentWord,String documentName, String count){
       // System.out.println("in function "+documentName);
        String responsemsg = "";
        String newresponse = "";
        System.out.println("document name is "+documentName);
        if (keySet.contains(currrentWord)) {
            //System.out.println(currrentWord+" contained");
            String requestmsg = "GET " + currrentWord + "\n";
            String updatedRequestValue = "";
            Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(outputDirectory).build();
            responsemsg = stub.serverOperation(request).getOutput();
           // System.out.println("before "+responsemsg);
            String[] msgs = responsemsg.split("\r\n");
            // System.out.println("response   " + responsemsg);
            if (!responsemsg.trim().equals("UNSUCCESSFUL")) {
                responsemsg = msgs[1].trim();
                String newValue= responsemsg + ","+propertiesFileHashMap.get(documentName)+":"+count;
                int noOfBytes = newValue.getBytes().length;
                String num = Integer.toString(noOfBytes);
                requestmsg = "SET " + currrentWord + " " + num + "\r\n" + newValue + "\r\n";
                request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(outputDirectory).build();
                newresponse = stub.serverOperation(request).getOutput();

            }
        } else {
            keySet.add(currrentWord);
            String requestValue = propertiesFileHashMap.get(documentName)+":"+count;
            int noOfBytes = requestValue.getBytes().length;
            String num = Integer.toString(noOfBytes);
            String requestmsg = "SET " + currrentWord + " " + num + "\r\n" + requestValue + "\r\n";
            Keyval.Request request = Keyval.Request.newBuilder().setInput(requestmsg).setFilepath(outputDirectory).build();
            String newResponse = stub.serverOperation(request).getOutput();
        }
    }


    private static void createList(){
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
            e.printStackTrace();
        }
    }

    private static void createDirectory(){
        File od = new File(outputDirectory);
        if(!od.exists()) od.mkdirs();
    }


    public static String checkarguments(String[] args){
        if(!args[0].equals("-i") || !args[2].equals("-d") || !args[4].equals("-o") || !args[6].equals("-p")){
            return "Arguments are invalid";
        }
        inputFileBucketPath = args[1];
        String dirlists = args[3];
        outputDirectory = args[5];
        inputPropertiesFileBucketPath =args[7];

        inputmapperDirectories = dirlists.split(",");
    /*
        if(!isPathValid(inputFileLocation)){
            return "Input file is not valid";
        }

        for (String dir : inputmapperDirectories) {
            System.out.println("current dir path "+dir);
            if(!isPathValid(dir)){
                return dir + " Output directory path is invalid";
            }
        }
     */
        LOGGER.info("Arguments are accepted");


        return "none";

    }

    public static boolean isPathValid(String path){
        File f = new File(path);
        return (f.exists());
    }

    public static void setProperties(){
        System.out.println("path of properties file in the reducer "+directoryToInputFilePath);
        try {
            File file = new File(directoryToInputFilePath);
            FileInputStream fis = new FileInputStream(file);
            directoryToInputFileProperties.load(fis);
            for (String key : directoryToInputFileProperties.stringPropertyNames()) {
                String value = directoryToInputFileProperties.getProperty(key);
                propertiesFileHashMap.put(key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String downloadFileFromBucket(String inputPath) {
        List<String> argumentList = new ArrayList<>();
        argumentList.add("gsutil");
        argumentList.add("cp");
        argumentList.add("gs://"+BUCKET_NAME+inputPath);
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
        int index = inputPath.lastIndexOf('/');
        String newIntermediateName = inputPath.substring(index+1);
        return System.getProperty("user.dir")+"/"+newIntermediateName;
    }


    public static void main(String[] arg){
        initLogger();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("111.106.185.35.bc.googleusercontent.com",1993).usePlaintext().build();
        stub = KeyValueGrpc.newBlockingStub(channel);
        if(arg.length!=8){
            System.out.println("Please give all arguments");
            return;
        }
        String check = checkarguments(arg);
        if(!check.equals("none")){
            System.out.println(check);
            return;
        }
        LOGGER.info("To download input file from bucket");
        inputFileLocation=downloadFileFromBucket(inputFileBucketPath);
        LOGGER.info("Downloaded input file from bucket");

        LOGGER.info("To Download .properties file from bucket");
        directoryToInputFilePath=downloadFileFromBucket(inputPropertiesFileBucketPath);
        LOGGER.info("Downloaded .properties file from bucket");
        LOGGER.info("Reading the properties file mapping containing the mapping of output directory to input file");
        setProperties();
        System.out.println("Everything is fine");
        LOGGER.info("Creating the directory to save reducer output");
        //createDirectory();
        //System.out.println("out dir "+outputDirectory);
        LOGGER.info("Parsing input file and loading it to internal data structure");
        createList();
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

}

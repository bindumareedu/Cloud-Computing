

import com.mapreduce.grpc.internalFiles.Mapreduce;
import com.mapreduce.grpc.internalFiles.MasterGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.zeroturnaround.exec.ProcessExecutor;

import javax.print.DocFlavor;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Formatter;
import java.util.logging.*;
import java.util.concurrent.TimeoutException;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;


public class MasterNode extends MasterGrpc.MasterImplBase {
    static HashMap<String,ArrayList<String>> documentToListOfWords = new HashMap<>();
    static HashSet<String> documentNameSet;
    static String OutputDirectory = "";
    static String UserOutputDirectory = "";
    static String UserInputFilePath = "";
    static Set<String> allWords = new HashSet<>();
    static ArrayList<String> inputFilesForMapper = new ArrayList<>();
    static String[] inputFilesForMaster;
    static List<String> outputDirectoriesForMapper = new ArrayList<>();
    static String mapReduceFunction = "";
    static Path masterOutputDirectory;
    static String javaHome ;
    static String classpath ;
    static String javaBin;
    static Properties jvm;
    static String mapperJarPath;
    static String reducerJarPath;
    static String outputFileWithAllWords;
    static String getOutputDirectoryForReducer;
    static String inputFilForReducer="";
    static Properties propertiesFileForIIReducer;
    static String nameOfPropertiesFileForIIReducer="dictionary.properties";
    static String finalOutputfileName = "finalOutput.properties";
    static final Logger LOGGER = Logger.getLogger(MasterNode.class.getName());
    static Handler consoleHandler = null;
    static Handler fileHandler = null;
    static SimpleFormatter simpleFormatter = null;
    static String RELATIVE_PATH = "/home/bindu/";
    static String PROJECT_ID = "641706133006";
    static String BUCKET_NAME ="mapreduce-storage";
    static int mapperstatus=0;

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

    @Override
    public void masterNode(com.mapreduce.grpc.internalFiles.Mapreduce.Request request,
                           io.grpc.stub.StreamObserver<com.mapreduce.grpc.internalFiles.Mapreduce.Response> responseObserver) {
        mapperstatus += request.getStatus();
        System.out.println("Mapper status "+mapperstatus);
    }

    public static ArrayList<String> parseFile(String filename){
        ArrayList<String> wordsinCurrentFile = new ArrayList<>();
       // File file = new File(getClass().getClassLoader().getResource("pp.txt").getFile());
        System.out.println(UserInputFilePath);

        java.nio.file.Path inputFilePath = java.nio.file.Paths.get(UserInputFilePath);
        File inputfile = new File(filename);
        String line ="";
        try {
            // scanner = new Scanner(file);
            BufferedReader bf = new BufferedReader(new FileReader(inputfile));
            while((line= bf.readLine() )!= null){
                //String line = scanner.nextLine();
                wordsinCurrentFile.addAll(createListOfWords(line)) ;
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while loading file",e);

        }
        return wordsinCurrentFile;

    }

    public static HashSet<String> createListOfWords(String line) {
         HashSet<String>  totalWords = new HashSet<>();
        String[] words = line.split("\\W+");
        for (String s : words) {
            s = s.toLowerCase();
            if (s.length() > 0 && Character.isLetterOrDigit(s.charAt(0)))
                totalWords.add(s);
        }
        return totalWords;
    }

    public static String writeToFile(List<String> totalWords,String inputDocName){
        java.nio.file.Path outputFilePath = java.nio.file.Paths.get(UserOutputDirectory,OutputDirectory);
        System.out.println(outputFilePath);
        if(!Files.exists(outputFilePath)) {
            try {
                Files.createDirectories(outputFilePath);
            } catch (IOException e) {
                //add comments to logger
                e.printStackTrace();
                LOGGER.log(Level.SEVERE, "Error while creating directories",e);

            }
        }
        int index = inputDocName.lastIndexOf('/');
        String newIntermediateName = inputDocName.substring(index+1);
        int index2 = newIntermediateName.indexOf(".");
        String out = newIntermediateName.substring(0,index2);
        System.out.println("intermediate name "+out);
        String outputFileName = out+"_output.txt";
        outputFilePath = java.nio.file.Paths.get(UserOutputDirectory,OutputDirectory,outputFileName);
        System.out.println(outputFilePath);

        File inputFile = new File(String.valueOf(outputFilePath));
        try {
            inputFile.createNewFile();
            inputFile.setExecutable(true);
            inputFile.setReadable(true);
            inputFile.setWritable(true);
            BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile.getPath()));
            for(String s: totalWords){
                if(s.length() > 0){
                    writer.write(s);
                    writer.write("\n");
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while writing to file",e);

        }
        return outputFilePath.toString();
    }

    public static void invertedIndexParser(){
        int i=0;
            for(String  documentName : inputFilesForMaster){
            String fileName= documentName;
            ArrayList<String> wordsInCurrentDocument = parseFile(fileName);
            allWords.addAll(wordsInCurrentDocument);
            documentToListOfWords.put(documentName,wordsInCurrentDocument);
                //it is storing complete path of inputfile for mapper
                inputFilesForMapper.add(writeToFile(wordsInCurrentDocument,documentName));
                uploadFileIncludingPath(inputFilesForMapper.get(i));
                i++;
        }
    }



    public static void getFinalOuputToLocal(){
       // gcloud compute scp mr.jar   master:/home/bindu/today
        LOGGER.info("Created list finalOutput.properties in user readable format");

//        Path path = Paths.get(getOutputDirectoryForReducer);
//        if(!path.toFile().exists()) path.toFile().mkdirs();
        LOGGER.info("Copying file from "+getOutputDirectoryForReducer);
        List<String> argumentList = new ArrayList<>();
        argumentList.add("gcloud");
        argumentList.add("compute");
        argumentList.add("scp");
        argumentList.add("--recurse");
        argumentList.add("kv-service:"+getOutputDirectoryForReducer);
        argumentList.add(getOutputDirectoryForReducer);
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
    }

    /* THIS FUNCTION NEEDS TO BE UPDATED */
    public static void makingOutputFile(){
        //allWords - set
        //getOutputDirectoryForReducer - reducer
        java.nio.file.Path finalOutputFilePath = java.nio.file.Paths.get(UserOutputDirectory,OutputDirectory,finalOutputfileName);
        System.out.println("output file name "+finalOutputFilePath.toString());
        System.out.println("input file to create output file "+getOutputDirectoryForReducer);
        LOGGER.info("creating final output at "+getOutputDirectoryForReducer);
        File outputfile = finalOutputFilePath.toFile();
        try {
            outputfile.createNewFile();
            FileInputStream fis = new FileInputStream(outputfile);
            Properties properties = new Properties();
            properties.load(fis);
            for(String word : allWords){
                File currentFile = new File(getOutputDirectoryForReducer+"/"+word+".txt");
                if(currentFile.exists()){
                    FileInputStream fs = new FileInputStream(currentFile);
                    BufferedReader bf = new BufferedReader(new InputStreamReader(fs));
                    String key = word;
                    String value = bf.readLine();
                    properties.setProperty(key,value);
                }
            }
            FileOutputStream fos = new FileOutputStream(outputfile);
            properties.store(fos, " Final output file");
        } catch (Exception e) {
            //e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while creating final output file",e);
        }
    }

    public static void wordCountParser(){
        LOGGER.info("Parsing the input");
            ArrayList<List<String>> wordCountList = new ArrayList<>();
            ArrayList<String> wordsInCurrentDocument;
            wordsInCurrentDocument = parseFile(inputFilesForMaster[0]);
            int totalNumberOfWordsPerMapper = wordsInCurrentDocument.size()/3;
            List<String> l1 = wordsInCurrentDocument.subList(0,totalNumberOfWordsPerMapper);
            List<String> l2 = wordsInCurrentDocument.subList(totalNumberOfWordsPerMapper,2*totalNumberOfWordsPerMapper);
            List<String> l3 = wordsInCurrentDocument.subList(2*totalNumberOfWordsPerMapper, wordsInCurrentDocument.size());
            wordCountList.add(l1);
            wordCountList.add(l2);
            wordCountList.add(l3);
            allWords.addAll(wordsInCurrentDocument);
            for(int i = 0; i < wordCountList.size();i++){
                //it is storing complete path of inputfile for mapper
                LOGGER.info("Creating data for mapper");
                String inputF =writeToFile( wordCountList.get(i), inputFilesForMaster[0]+i);
                inputFilesForMapper.add(inputF);
                LOGGER.info("Uploading data for mapper on the bucket "+inputFilesForMapper.get(i));
                uploadFileIncludingPath(inputFilesForMapper.get(i));
                LOGGER.info("Created list of words for Mapper: "+inputFilesForMapper.get(i));
            }

    }

    private static void uploadFileIncludingPath(String filepath) {
        LOGGER.info("Uploading path to the bucket "+filepath);
        List<String> argumentList = new ArrayList<>();
        argumentList.add("gsutil");
        argumentList.add("cp");
        argumentList.add("-r");
        argumentList.add(filepath);
        argumentList.add("gs://"+BUCKET_NAME+filepath);
      //  LOGGER.info("Calling mapper function with command: "+argumentList);
        try {
            String output = new ProcessExecutor().command(argumentList)
                    .readOutput(true).execute()
                    .outputUTF8();
            System.out.println(output);


        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while uploading filepath to bucket",e);

        }
    }

    public static void combinedToOneFile(){
        int index = inputFilesForMaster[0].lastIndexOf("/");
        String path = System.getProperty("user.dir");
        outputFileWithAllWords = path+"/allwords.txt";
        List<String> in = new ArrayList<>(allWords);
        outputFileWithAllWords = writeToFile(in, outputFileWithAllWords);
        uploadFileIncludingPath(outputFileWithAllWords);
    }


    //////////
    public static boolean isPathValid(String path){
        File f = new File(path);
        return (f.exists());
    }

    public static String checkarguments(String[] args){

        if(!args[0].equals("-m") || !args[2].equals("-r") || !args[4].equals("-i") || !args[6].equals("-o")){

            return "Arguments are invalid";
        }
        for(String a : args) System.out.println(a);

        String inputFile = args[5];
        inputFilesForMaster = inputFile.split(",");
        LOGGER.info("Number of input files: "+inputFilesForMaster.length+" inputFile: "+inputFile);

        UserOutputDirectory = args[7];
        mapperJarPath = args[1];
        reducerJarPath = args[3];
        uploadFileIncludingPath(mapperJarPath);
        uploadFileIncludingPath(reducerJarPath);
//        if(args[1].equals("wc")){
        if(inputFilesForMaster.length == 1){
//            if(!isPathValid(inputFile) || !isPathValid(mapperJarPath) || !isPathValid(reducerJarPath) ){
//                return "One of the file paths is not valid";
//            }
//            if(!isPathValid(UserOutputDirectory)){
//                return "Output file not valid";
//            }
            String timeStamp = new SimpleDateFormat("yyyy_MM_dd_HH_mm").format(new java.util.Date());
            OutputDirectory = "MapReduceOutput_"+timeStamp;
            mapReduceFunction = "wc";
        }
//        else if(args[1].equals("ii")){
        else if(inputFilesForMaster.length > 1){
//            if(!isPathValid(UserOutputDirectory)){
//                return "Output file not valid";
//            }
//            for(String file:inputFilesForMaster){
//                if(!isPathValid(file) || !isPathValid(mapperJarPath) || !isPathValid(reducerJarPath) ){
//                    return "One of the file paths is not valid";
//                }

            //}
            String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm").format(new java.util.Date());
            OutputDirectory = "MapReduceOutput_"+timeStamp;
            mapReduceFunction = "ii";
        }
        else{
            return "Arguments are invalid";
        }

        return "none";

    }
    /////////////

    public static void init_master(){
        LOGGER.info("Initialised master");
        // if(mapReduceFunction.equals("wc")){
        ArrayList<String> mapperinstances = new ArrayList<>();
        ArrayList<String> reducerinstances = new ArrayList<>();
        if(inputFilesForMaster.length == 1){
            LOGGER.info("Input files is only one. So diving the input for each of the mappers.");
            wordCountParser();
            createOutputDirectoriesForMapper();
           // createOutputDirectoriesInKVStore();
            for(int i = 0; i < outputDirectoriesForMapper.size() ;i++){
                LOGGER.info("Created Mapper Process: "+i);
               launchMapperProcess(inputFilesForMapper.get(i),outputDirectoriesForMapper.get(i),"mapper"+i);
                mapperinstances.add("mapper"+i);
            }
            while(mapperstatus != outputDirectoriesForMapper.size()){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            LOGGER.info("Mappers done");
            deleteInstances(mapperinstances);
            inputFilForReducer = String.join(",",outputDirectoriesForMapper);
            combinedToOneFile();
            LOGGER.info("Created list of all the words for inputting to the reducer");
            System.out.println("input file for reducer before calling mapper  ->  " +inputFilForReducer);
            launchReducerProcess(outputFileWithAllWords,inputFilForReducer,getOutputDirectoryForReducer);
            int prevcount = mapperstatus+1;
            reducerinstances.add("reducer");
            while(mapperstatus != prevcount){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            LOGGER.info("Reducer done");
            deleteInstances(reducerinstances);
            getFinalOuputToLocal();
            makingOutputFile();
            LOGGER.info("Created list finalOutput.properties in user readable format");

            //}
        }
        //  else if (mapReduceFunction.equals("ii")){
        else if(inputFilesForMaster.length >1){
            LOGGER.info("Map reduce operation : Inverted Index");
            invertedIndexParser();
            createOutputDirectoriesForMapper();
           // createOutputDirectoriesInKVStore();
            createPropertiesFile();
            inputFilForReducer = String.join(",",outputDirectoriesForMapper);
            //launch mapper processes
            for(int i = 0; i < outputDirectoriesForMapper.size() ;i++){
                LOGGER.info("Created Mapper Process: "+i);
                launchMapperProcess(inputFilesForMapper.get(i),outputDirectoriesForMapper.get(i),"mapper"+i);
                mapperinstances.add("mapper"+i);
            }
            while(mapperstatus != outputDirectoriesForMapper.size()){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            LOGGER.info("Mappers done");
            deleteInstances(mapperinstances);
            combinedToOneFile();
            LOGGER.info("Created list of all the words for inputting to the reducer");
            String pathToPropertiesFile = propertiesFileForIIReducer +"/"+nameOfPropertiesFileForIIReducer;
            Path path = Paths.get(UserOutputDirectory,OutputDirectory,nameOfPropertiesFileForIIReducer);
            String propFilePath =path+"/"+nameOfPropertiesFileForIIReducer;

            uploadFileIncludingPath(path.toString());
            System.out.println("path to prop file "+path+" name of file "+nameOfPropertiesFileForIIReducer);
            launchIIReducerProcess(outputFileWithAllWords,inputFilForReducer,getOutputDirectoryForReducer,path.toString());
            int prevcount = mapperstatus+1;
            reducerinstances.add("reducer");
            while(mapperstatus != prevcount){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            LOGGER.info("Reducer done");
            deleteInstances(reducerinstances);
            getFinalOuputToLocal();
            makingOutputFile();
            LOGGER.info("Created list finalOutput.properties in user readable format");

        }
        else ;
    }

    public static void createPropertiesFile() {

        String path = masterOutputDirectory+"/"+nameOfPropertiesFileForIIReducer;
        File file = new File(path);
        FileInputStream fis;
        propertiesFileForIIReducer = new Properties();

        try {
                file.createNewFile();
                file.setExecutable(true);
                file.setReadable(true);
                file.setWritable(true);
                fis = new FileInputStream(file);
                propertiesFileForIIReducer.load(fis);
            for(int i = 0; i < outputDirectoriesForMapper.size(); i++){
                int index = inputFilesForMaster[i].lastIndexOf("/");
                String textfilename = inputFilesForMaster[i].substring(index+1);
                propertiesFileForIIReducer.setProperty(outputDirectoriesForMapper.get(i),textfilename);
                FileOutputStream fileOut = new FileOutputStream(file);
                propertiesFileForIIReducer.store(fileOut,"updated the dictionary values for output directories and input files");
            }
            } catch (IOException e) {
                //e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while creating properties file with mapping of mapper output directory to input file",e);

        }
        LOGGER.info("Created dictionary.properties at: "+path);
    }

    public static void deleteInstances(ArrayList<String> instancenames){
        List<String> argumentList = new ArrayList<>();
        argumentList.add("gcloud");
        argumentList.add("compute");
        argumentList.add("instances");
        argumentList.add("delete");
        argumentList.addAll(instancenames);
        argumentList.add("--zone");
        argumentList.add("us-east1-b");
        try {
            String output = new ProcessExecutor().command(argumentList)
                    .readOutput(true).execute()
                    .outputUTF8();
            System.out.println(output);
            LOGGER.log(Level.INFO, "Deleted instance");

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while deleting mapper instances",e);

        }
    }

    public static void launchMapperProcess(String inputLocation,String outputLocation ,String mappername){
        List<String> argumentList = new ArrayList<>();
//        argumentList.add(javaBin);
//        argumentList.add("-cp");
//        argumentList.add(classpath);
//        argumentList.add("-jar");
//        argumentList.add(mapperJarPath);
//        argumentList.add("-i");
//        argumentList.add(inputLocation);
//        argumentList.add("-o");
//        argumentList.add(outputLocation);
        argumentList.add("python3");
        argumentList.add("create_mapper.py");
        argumentList.add(PROJECT_ID);
        argumentList.add(BUCKET_NAME);
        argumentList.add("--name");
        argumentList.add(mappername);
        argumentList.add("--inputfilepath");
        argumentList.add(inputLocation);
        argumentList.add("--outputdirectory");
        argumentList.add(outputLocation);
        argumentList.add("--jarpath");
        argumentList.add(mapperJarPath);
        LOGGER.info("Calling mapper function with command: "+argumentList);
        try {
            String output = new ProcessExecutor().command(argumentList)
                    .readOutput(true).execute()
                    .outputUTF8();
            System.out.println(output);

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while launching mapper process",e);

        }
    }


    public static void launchReducerProcess(String inputTextFile, String inputDir, String outputDir ){
        List<String> argumentList = new ArrayList<>();
        argumentList.add("python3");
        argumentList.add("create_reducers.py");
        argumentList.add(PROJECT_ID);
        argumentList.add(BUCKET_NAME);
        argumentList.add("--name");
        argumentList.add("reducer");
        argumentList.add("--inputfiles");
        argumentList.add(inputTextFile);
        argumentList.add("--inputdirs");
        argumentList.add(inputDir);
        argumentList.add("--outputdir");
        argumentList.add(outputDir);
        argumentList.add("--jarpath");
        argumentList.add(reducerJarPath);

        LOGGER.info("Calling reducer function with command: "+argumentList);
        try {
            String output = new ProcessExecutor().command(argumentList)
                    .readOutput(true).execute()
                    .outputUTF8();
            System.out.println(output);
        } catch (Exception e) {
           // e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while launching reducer process",e);

        }
    }

    public static void launchIIReducerProcess(String inputTextFile, String inputDir, String outputDir,String propertiesFilePath ){
        List<String> argumentList = new ArrayList<>();
        argumentList.add("python3");
        argumentList.add("create_iireducer.py");
        argumentList.add(PROJECT_ID);
        argumentList.add(BUCKET_NAME);
        argumentList.add("--name");
        argumentList.add("reducer");
        argumentList.add("--inputfiles");
        argumentList.add(inputTextFile);
        argumentList.add("--inputdirs");
        argumentList.add(inputDir);
        argumentList.add("--outputdir");
        argumentList.add(outputDir);
        argumentList.add("--propfile");
        argumentList.add(propertiesFilePath);
        argumentList.add("--jarpath");
        argumentList.add(reducerJarPath);
        System.out.println(masterOutputDirectory+"/"+nameOfPropertiesFileForIIReducer);
        LOGGER.info("Calling reducer function with command: "+argumentList);

        try {
            String output = new ProcessExecutor().command(argumentList)
                    .readOutput(true).execute()
                    .outputUTF8();
            System.out.println(output);
        } catch (Exception e) {
           // e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Error while launching mapper process",e);

        }
    }

    private static void createOutputDirectoriesForMapper() {
      //  for(String inputMapper : inputFilesForMapper){
            for(int i = 0; i < inputFilesForMapper.size();i++){
            String mapperOutputDirectory= UserOutputDirectory+OutputDirectory+"/"+"mapper"+i;
                outputDirectoriesForMapper.add(mapperOutputDirectory);
        }
            LOGGER.info("created output directory paths for each mappers \n"+outputDirectoriesForMapper);
            getOutputDirectoryForReducer=UserOutputDirectory+OutputDirectory+"/reducer";

    }

    public static void main(String[] arg){
        jvm = System.getProperties();
        javaHome = jvm.getProperty("java.home");
        classpath = jvm.getProperty("java.class.path");
        javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        initLogger();
        //createOutputDirectoriesInKVStore();

        for(String a : arg) System.out.println(a);
        if(arg.length!=8) System.out.println("Please give all the arguments");
        else {
            String check = checkarguments(arg);
            if (!check.equals("none")) {
                System.out.println(check);
                return;
            }
        }
        LOGGER.info("Arguments checked");
        LOGGER.info("Generating output directories for mapper");
        masterOutputDirectory = Paths.get(UserOutputDirectory,OutputDirectory);
        //createOutputDirectoriesForMapper();
        //createOutputDirectory();
        LOGGER.info("Initializing master");
        Server server = ServerBuilder.forPort(1992).addService(new MasterNode()).build();
        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        init_master();

//        for(String file : inputFilesForMapper){
//            System.out.println(file);
//        }


    }

    private static void createOutputDirectory() {
        masterOutputDirectory = Paths.get(UserOutputDirectory,OutputDirectory);
       if(!masterOutputDirectory.toFile().exists()) {
           masterOutputDirectory.toFile().mkdirs();
       }
       System.out.println("in create outputdir "+masterOutputDirectory.toString());

    }

    private static void createOutputDirectoriesInKVStore(){
       // for(String outputDirectoryForMapper : outputDirectoriesForMapper ){
            //create respective directory in KVSTORE
            // new path should be Paths.get(RELATIVE_PATH,OutputDirectory,outputDirectoryForMapper)
            //outputDirectoryForMapper = update it with new path.
            List<String> argumentList = new ArrayList<>();
            argumentList.add("gcloud");
            argumentList.add("compute");
            argumentList.add("ssh");
            argumentList.add("kv-server");
            argumentList.add(";");
            //argumentList.add("ls");
            LOGGER.info("Calling reducer function with command: "+argumentList);
            try {
                String output = new ProcessExecutor().command(argumentList)
                        .readOutput(true).execute()
                        .outputUTF8();
                List<String> arg = new ArrayList<>();
                arg.add("ls");
                String o = new ProcessExecutor().command(arg)
                        .readOutput(true).execute()
                        .outputUTF8();
                System.out.println(o);
            } catch (Exception e) {
                // e.printStackTrace();
                LOGGER.log(Level.SEVERE, "Error while launching reducer process",e);

            }

//        }
        //create reducer output directory
        //Paths.get(RELATIVE_PATH,OutputDirectory,"reducer")
    }
}

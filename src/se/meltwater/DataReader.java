package se.meltwater;


import com.meltwater.quiddity.factory.QuiddityObjectFactory;
import com.meltwater.quiddity.generated.document.Document;
import com.meltwater.quiddity.generated.enrichments.NamedEntity;
import com.meltwater.quiddity.generated.source.Source;
import com.meltwater.quiddity.impl.DefaultQuiddityObjectFactory;
import com.meltwater.quiddity.impl.JsonQuiddityObjectSerializer;
import com.meltwater.quiddity.support.QuiddityObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * // TODO class description
 */
public class DataReader {
    private HashMap<String,Integer> entities = new HashMap<>();
    private int nextId = 0;
    private PrintWriter translationFileWriter;
    private static final String translationFileFolder = "files";
    private static final String translationFile       = translationFileFolder + "/translation.txt";

    private static final String documentsPath         = "/home/johan/programming/meltwater/data/documents/";
    private static final String contactsPath          = "/home/johan/programming/meltwater/data/contacts/";
    private static final String sourcesPath           = "/home/johan/programming/meltwater/data/sources/";

    private int progress = 0;
    private int progressUpdateInterval = 100;

    private JsonQuiddityObjectSerializer serializer = new JsonQuiddityObjectSerializer();
    private QuiddityObjectFactory qof = new DefaultQuiddityObjectFactory();

    enum SyncStates {WritingToTransFile, PuttingNextEntity};

    public DataReader(){
        try {

            File file = createOrGetTranslationFile();

            translationFileWriter = new PrintWriter(file);
            translationFileWriter.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private File createOrGetTranslationFile() throws IOException {
        File folder = new File(translationFileFolder);
        File file = new File(translationFile);

        if(!folder.exists()) {
            System.out.println("Missing folder, creating it for you");
            folder.mkdirs();
        }

        if(!file.exists()) {
            System.out.println("Missing translation file, creating it for you");
            file.createNewFile();
        }

        return file;
    }

    public void run() throws InterruptedException, IOException {
        readDocumentFiles(documentsPath);
        readSourcesFiles(sourcesPath);
    }


    public QuiddityObject parseJson(String fileName) throws IOException {
        QuiddityObject object;

        try(InputStream instream = new FileInputStream(fileName)){
            object = serializer.read(instream, qof );

        }

        return object;
    }

    public synchronized void progress() {
        if(progress++ % progressUpdateInterval == 0 ) {
            System.out.println("DataReader progress " + progress);
        }
    }

    public synchronized int getNextId() {
        return nextId++;
    }

    private void readFiles(FileParser fileReader, String documentsPath) throws InterruptedException {

        progress = 0;
        File allArticles = new File(documentsPath);

        ExecutorService threadPool = Executors.newFixedThreadPool(8);

        System.out.println("DataReader adding threads to pool");
        for(File file : allArticles.listFiles()){
            threadPool.submit(() -> {
                try {
                    fileReader.apply(file);
                    progress();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        threadPool.shutdown();
        System.out.println("DataReader awaiting threads to finish");
        threadPool.awaitTermination(20, TimeUnit.HOURS);
    }

    private void readDocumentFiles(String documentsPath) throws InterruptedException {

        readFiles((File f) -> readDocumentFile(f),documentsPath);
        translationFileWriter.close();
    }

    private void readDocumentFile(File file) throws IOException {
        Document document = (Document)parseJson(file.getAbsolutePath());

        int articleId = getNextId();
        HashMap<String, Integer> entityNamesToId = new HashMap<>();

        for(NamedEntity namedEntity : document.createOrGetEnrichments().createOrGetNamedEntities()){
            if(namedEntity.getName() == null){
                continue;
            }

            int entityId;
            synchronized (SyncStates.PuttingNextEntity) {
                Integer existed = entities.putIfAbsent(namedEntity.getName(), nextId);
                entityId = existed == null ? getNextId() : existed;
            }
            entityNamesToId.put(namedEntity.getName(), entityId);
        }

        synchronized (SyncStates.WritingToTransFile) {
            translationFileWriter.println(articleId + " file: " + file.getName() + " article: " + document.getId());
            for (Map.Entry<String, Integer> mapEntry : entityNamesToId.entrySet()) {
                translationFileWriter.println("\t" + mapEntry.getValue() + " " + mapEntry.getKey());
            }
        }
    }


    private void readSourcesFiles(String sourcesPath) throws InterruptedException {
        readFiles((File f) -> readSourceFile(f),sourcesPath);
    }


    private void readSourceFile(File file) throws IOException {
        Source source = (Source)parseJson(file.getAbsolutePath());
        source.createOrGetBeats();
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        DataReader reader = new DataReader();
        reader.run();
    }

    interface FileParser{
        void apply(File file) throws IOException;
    }
}
package se.meltwater;

import com.meltwater.quiddity.factory.QuiddityObjectDeserializer;
import com.meltwater.quiddity.factory.QuiddityObjectFactory;
import com.meltwater.quiddity.generated.document.Document;
import com.meltwater.quiddity.generated.enrichments.NamedEntity;
import com.meltwater.quiddity.impl.DefaultQuiddityObjectFactory;
import com.meltwater.quiddity.impl.JsonQuiddityObjectSerializer;
import com.meltwater.quiddity.support.QuiddityObject;


import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataReader {
    private HashMap<String,Integer> entities = new HashMap<>();
    private int nextId = 0;
    private PrintWriter translationFileWriter;
    private static final String translationFileFolder = "files";
    private static final String translationFile       = translationFileFolder + "/translation.txt";

    private int progress = 0;

    private JsonQuiddityObjectSerializer serializer = new JsonQuiddityObjectSerializer();
    private QuiddityObjectFactory qof = new DefaultQuiddityObjectFactory();

    enum SyncStates {WritingToTransFile, PuttingNextEntity};

    public DataReader(){
        try {
            File folder = new File(translationFileFolder);
            File file = new File(translationFile);

            if(!folder.exists()) {
                folder.mkdirs();
            }

            if(!file.exists()) {
                file.createNewFile();
            }

            translationFileWriter = new PrintWriter(file);
            translationFileWriter.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run(String articlesFolder) throws InterruptedException, IOException {
        File allArticles = new File(articlesFolder);

        ExecutorService threadPool = Executors.newFixedThreadPool(8);

        System.out.println("DataReader adding threads to pool");
        for(File file : allArticles.listFiles()){
             threadPool.submit(() -> {
                try {
                    readFile(file);
                    progress();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        threadPool.shutdown();
        System.out.println("DataReader awaiting threads to finish");
        threadPool.awaitTermination(20, TimeUnit.HOURS);

        translationFileWriter.close();
    }


    public Document parseJson(String fileName) throws IOException {
        Document document;

        try(InputStream instream = new FileInputStream(fileName)){
            QuiddityObject object = serializer.read(instream, qof );

            document = (Document)object;
        }

        return document;
    }

    public synchronized void progress() {
        if(progress++ % 1000 == 0 ) {
            System.out.println("DataReader progress " + progress);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        DataReader reader = new DataReader();
        reader.run("/media/johan/Data/meltwater/20160110-bite/output");
    }

    public synchronized int getNextId() {
        return nextId++;
    }

    private void readFile(File file) throws IOException {
        Document document = parseJson(file.getAbsolutePath());
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
}
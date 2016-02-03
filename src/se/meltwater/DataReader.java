package se.meltwater;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataReader {
    private HashMap<String,Integer> entities = new HashMap<>();
    private int nextId = 0;
    private PrintWriter translationFileWriter;
    private static final String translationFile = "files/translation.txt";
    private int progress = 0;

    enum SyncStates {WritingToTransFile, PuttingNextEntity};

    public DataReader(){
        try {
            translationFileWriter = new PrintWriter(new File(translationFile));
            translationFileWriter.flush();

        } catch (FileNotFoundException e) {
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


    public Document parseGson(String fileName) throws IOException {
        Document document;
        try(Reader reader = new InputStreamReader(new FileInputStream(fileName))){
            Gson gson = new GsonBuilder().create();
            document = gson.fromJson(reader, Document.class);
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
        Document document = parseGson(file.getAbsolutePath());

        if(!document.containsNecessaryData()) {
            return;
        }

        int articleId = getNextId();
        HashMap<String, Integer> entityNamesToId = new HashMap<>();

        for(NamedEntities namedEntity : document.enrichments.namedEntities){
            if(namedEntity.name == null){
                continue;
            }

            int entityId;
            synchronized (SyncStates.PuttingNextEntity) {
                Integer existed = entities.putIfAbsent(namedEntity.name, nextId);
                entityId = existed == null ? getNextId() : existed;
            }
            entityNamesToId.put(namedEntity.name, entityId);
        }

        synchronized (SyncStates.WritingToTransFile) {
            translationFileWriter.println(articleId + " file: " + file.getName() + " article: " + document.id);
            for (Map.Entry<String, Integer> mapEntry : entityNamesToId.entrySet()) {
                translationFileWriter.println("\t" + mapEntry.getValue() + " " + mapEntry.getKey());
            }
        }
    }


    /* GSON Parsing classes */
    public class Document {
        String id;
        Enrichments enrichments;

        public boolean containsNecessaryData( ){
            return id != null && enrichments != null && enrichments.namedEntities != null;
        }
    }

    public class Enrichments{
        public NamedEntities[] namedEntities;
    }

    public class NamedEntities{
        String name;
    }
}
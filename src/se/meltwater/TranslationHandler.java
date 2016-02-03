package se.meltwater;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by johan on 2016-01-29.
 */

public class TranslationHandler {

    private static final String translationFile = "files/translation.txt";
    private static final String arcsFile = "files/graphSubset.csv";
    TreeMap<Integer, Set<Integer>> entityToArticles = new TreeMap<>();
    private int progress = 0;

    public static void main(String[] args) throws FileNotFoundException {
        TranslationHandler  translationReader = new TranslationHandler ();
        translationReader.run();
    }

    public void run() throws FileNotFoundException {
        System.out.println("Started parsing file");
        parseTranslationFile();
        System.out.println("Parsing completed");

        System.out.println("Started writing arcs file");
        printArcsFromTranslation();
        System.out.println("Writing completed");
    }

    public void parseTranslationFile() throws FileNotFoundException {
        Scanner scan = new Scanner(new File(TranslationHandler.translationFile));

        int i = 0;
        while(scan.hasNext()){
            int art = scan.nextInt();

            scan.nextLine();

            while (scan.findInLine("\\t") != null && scan.hasNext()) {
                int entityId = scan.nextInt();
                scan.nextLine();

                Set<Integer> entityArticles = entityToArticles.get(entityId);

                if(entityArticles == null) {
                    entityArticles = new TreeSet<>();
                }

                entityArticles.add(art);

                entityToArticles.put(entityId, entityArticles);

            }
            progress();
            if(i++ > 300) {
                break;
            }
        }

        scan.close();
    }

    public void printArcsFromTranslation() throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(new File(arcsFile));
        writer.flush();

        for(Map.Entry<Integer, Set<Integer>> entry : entityToArticles.entrySet()) {
            Integer key = entry.getKey();
            for(Integer article : entry.getValue()) {
                writer.println(key + ";" + article);
            }
            progress();
        }
        writer.close();
    }

    public synchronized void progress() {
        if(progress++ % 1000 == 0 ) {
            System.out.println("Translation progress " + progress);        }
    }
}

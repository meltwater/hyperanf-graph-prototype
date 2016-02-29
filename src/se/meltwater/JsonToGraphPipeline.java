package se.meltwater;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by johan on 2016-01-29.
 */
public class JsonToGraphPipeline {

    public static void main(String[] args) throws InterruptedException, IOException {
        DataReader dataReader = new DataReader();
        dataReader.run();

        TranslationHandler translationHandler = new TranslationHandler();
        translationHandler.run();

        Converter converter = new Converter();
        //converter.convert();
    }
}

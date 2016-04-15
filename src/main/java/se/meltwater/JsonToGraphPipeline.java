package se.meltwater;

import java.io.IOException;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * // TODO class description
 */public class JsonToGraphPipeline {

    public static void main(String[] args) throws InterruptedException, IOException {
        DataReader dataReader = new DataReader();
        dataReader.run();

        TranslationHandler translationHandler = new TranslationHandler();
        translationHandler.run();

        Converter converter = new Converter();
        //converter.convert();
    }
}

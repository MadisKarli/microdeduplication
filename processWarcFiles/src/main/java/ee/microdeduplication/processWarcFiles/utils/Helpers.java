package ee.microdeduplication.processWarcFiles.utils;

/**
 * Created by Madis-Karli Koppel on 15/01/2017.
 */
public class Helpers {
    public static boolean inList(String inputStr, String[] items) {
        for (int i = 0; i < items.length; i++) {
            if (inputStr.contains(items[i])) {
                return true;
            }
            if (items[i].contains(inputStr)) {
                return true;
            }
        }
        return false;
    }
}

package classification;

import ws.palladian.retrieval.DocumentRetriever;

public class Test {
    public static void main(String[] args) {
        DocumentRetriever documentRetriever = new DocumentRetriever();

        documentRetriever.test();

        //String text = documentRetriever.getText("https://www.google.de");
        //System.out.println(text);
    }
}

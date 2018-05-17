package ee.microdeduplication.processWarcFiles.utils;

public class SimpleTuple {
    public String mime;
    public Integer size;
    public String comment;

    public SimpleTuple(String mime, Integer size, String comment){
        this.mime = mime;
        this.size = size;
        this.comment = comment;
    }

    @Override
    public String toString(){
        return this.mime + " | " + this.size + " | " + this.comment;
    }
}

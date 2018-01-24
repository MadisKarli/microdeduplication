package ee.thesis.models;

import java.util.HashMap;
import java.util.Map;

public class Entity {
    public String type;
    public String url;
    // maybe not needed at all
    public String any23id;
    public Map fields;

    public Entity(){}

    public Entity(String type, String url){
        this.type = type;
        this.url = url;
    }

    @Override
    public String toString(){
        return "Entity " + this.type
                + " from " + this.url
                + " " + this.any23id
                + "\n" + this.fields;
    }
    
}

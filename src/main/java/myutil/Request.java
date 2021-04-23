package myutil;
import java.io.Serializable;

public class Request implements Serializable{
    private static final long serialVersionUID = 1L;

    private int message_id;
    private int site_id;
    public Request(int message_id, int site_id){
        this.message_id = message_id;
        this.site_id = site_id;
    }
    public Request(){
    }
    public int get_message_id() {
        return message_id;
    }
    public void set_message_id(int message_id) {
        this.message_id = message_id;
    }
    public int get_site_id() {
        return site_id;
    }
    public void set_site_id(int site_id) {
        this.site_id = site_id;
    }
}
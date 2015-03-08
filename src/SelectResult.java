public class SelectResult {
	public String id_str;
    public String created_at;
    public String text;
    public String longitude;
    public String latitude;
    
    public SelectResult(String id_str){
    	this.id_str = id_str;
    }
    
    public void setTime(String created_at){
    	this.created_at = created_at;
    }
    
    public void setCoor1(String c) {
    	this.longitude = c;
    }
    
    public void setCoor2(String c) {
    	this.latitude = c;
    }
    
    public void setText(String text) {
    	this.text = text;
    }
	@Override
	public String toString() {
		return "SelectResult [id_str=" + id_str + ", created_at=" + created_at
				+ ", text=" + text + ", longitude=" + longitude + ", latitude=" + latitude
				+ "]";
	}
    
}
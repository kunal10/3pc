package dc;

/**
 * Class to represent song entity.
 */
public class Song {
  public Song(String name, String url) {
    this.name = name;
    this.url = url;
  }
  
  public String getName() {
    return name;
  }
  public String getUrl() {
    return url;
  }
  public void setName(String name) {
    this.name = name;
  }
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * Used by contains method of list of songs.
   * Assumes that there can be only 1 song with a particular name.
   */
  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof Song)) {
      return false;
    }
    Song otherSong = (Song) other;
    if (this.name.compareTo(otherSong.getName()) == 0) {
      return true;
    } 
    return false;
  }

  @Override
  public String toString() {
    return name+"^"+url;
  }
  
  public static Song parseSong(String s){
    String[] split = s.split("\\^");
    return new Song(split[0], split[1]);
  }
  
  private String name;
  private String url;
}

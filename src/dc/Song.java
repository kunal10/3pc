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
   */
  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof Song)) {
      return false;
    }
    Song otherSong = (Song) other;
    if (this.name == otherSong.getName() && this.url == otherSong.getUrl()) {
      return true;
    } 
    return false;
  }

  private String name;
  private String url;
}

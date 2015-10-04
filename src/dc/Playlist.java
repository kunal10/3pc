package dc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is a wrapper class for a list of songs. It maintains a synchronized list
 * of songs and provides methods to add, edit and delete from this list.
 */
public class Playlist {
  /**
   * Creates an empty playlist.
   */
  public Playlist() {
    this.songs = Collections.synchronizedList(new ArrayList<Song>());
  }
  
  /**
   * Adds the passed song to the playlist. Does nothing if the song is already
   * there.
   * @param s : Song to be added to the playlist.
   * @returns : Returns true if the song has been added to the playlist as a 
   *            result of this call, false otherwise.
   */
  public boolean addSong(Song s) {
    if (songs.contains(s)) {
      return false;
    }
    songs.add(s);
    return true;
  }
  
  /**
   * Edits the passed song in the playlist and updates its content with passed
   * values. Does nothing if the passed song is not present in the playlist.
   * 
   * @param s : Song to be modified.
   * @param newName : New name of the song. 
   * @param url : New url of the song.
   * @return : Returns true if the song is edited as a result of this call,
   *           false otherwise.
   */
  public boolean editSong(String oldName, String newName, String newUrl) {
    if (deleteSong(oldName)) {
      Song s = new Song(newName, newUrl);
      return addSong(s);
    } 
    return false;
  }
  
  /**
   * Deletes the song if found in the playlist. Does nothing if the song is not
   * found.
   * @param s : Song to be deleted from the current list.
   * @return : Returns true if the list is changed as a result of this call,
   *           false otherwise. 
   */
  public boolean deleteSong(String name) {
    Song s = new Song(name, "");
    if (songs.contains(s)) {
      return songs.remove(s);
    }
    return false;
  }
  
  /**
   * List of songs in this playlist.
   * 
   * NOTE : 
   * 1) List is initially empty.
   * 2) Duplicate songs are not allowed.
   * 3) This list should be synchronized so that if multiple threads are
   *    trying to access it then we don't get any concurrency issues. 
   */
  private List<Song> songs;
}

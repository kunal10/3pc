package dc;

import java.io.Serializable;

/**
 * Abstract class which captures basic functionalities of any message which is 
 * exchanged between different processes. Messages are classified on the basis
 * of process type of sender and receiver. Currently we have 2 kinds of 
 * messages:
 * 
 * 1. ControllerMessage
 * 2. ProcessMessage
 * 
 * All message classes are expected to extend this abstract class.
 */
public class Message{

  /**
   * 
   */
  public Message() {
  }

}

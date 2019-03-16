package lib;

import java.io.Serializable;
/**
 * This class is a wrapper for packing all the result information that you
 * might use in your own implementation of the RequestVote call, and also
 * should be serializable to return by remote function call.
 *
 */
public class AppendEntriesReply implements Serializable {
	public int term;
	public boolean success;
	
    public AppendEntriesReply(int term, boolean success) {
    	this.term = term;
    	this.success = success;
    }
}

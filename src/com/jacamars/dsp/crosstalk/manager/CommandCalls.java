package com.jacamars.dsp.crosstalk.manager;

import com.jacamars.dsp.rtb.commands.BasicCommand;


/** 
 * Interface for 0MQ based command call backs 
 * @author Ben M. Faul
 *
 */
public interface CommandCalls {
	public void callBack(BasicCommand cmd);
}

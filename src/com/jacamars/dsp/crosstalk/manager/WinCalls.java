package com.jacamars.dsp.crosstalk.manager;

import com.jacamars.dsp.rtb.pojo.WinObject;

/**
 * Interface that must be implemented by any class processing wins from 0MQ.
 * @author ben
 *
 */
public interface WinCalls {

	public void callBack(WinObject msg);
}

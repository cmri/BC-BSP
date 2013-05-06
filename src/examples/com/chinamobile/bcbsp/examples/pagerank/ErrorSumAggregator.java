package com.chinamobile.bcbsp.examples.pagerank;
/**
 * ErrorSumAggregator.java
 */
import java.util.Iterator;

import com.chinamobile.bcbsp.api.Aggregator;

/**
 * ErrorSumAggregator.java
 * This is used to aggregate the global Error.
 * 
 * @author WangZhigang
 * @version 0.1 2012-2-17
 */

public class ErrorSumAggregator extends Aggregator<ErrorAggregateValue> {
	
	@Override
	public ErrorAggregateValue aggregate(Iterable<ErrorAggregateValue> aggValues) {
		ErrorAggregateValue errorSum = new ErrorAggregateValue();
		Iterator<ErrorAggregateValue> it = aggValues.iterator();
		double errorValue = 0.0;
		
		while (it.hasNext()) {
			errorValue += Double.parseDouble(it.next().getValue());
		}
		errorSum.setValue(Double.toString(errorValue));
		
		return errorSum;
	}
}

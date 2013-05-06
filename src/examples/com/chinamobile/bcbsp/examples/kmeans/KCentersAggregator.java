/**
 * KCentersAggregator.java
 */
package com.chinamobile.bcbsp.examples.kmeans;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chinamobile.bcbsp.api.Aggregator;

/**
 * KCentersAggregator
 * 
 * @author Bai Qiushi
 * @version 0.1 2012-2-28
 */
public class KCentersAggregator extends Aggregator<KCentersAggregateValue> {

	public static final Log LOG = LogFactory.getLog(KCentersAggregator.class);
	
	@SuppressWarnings("unchecked")
	@Override
	public KCentersAggregateValue aggregate(
			Iterable<KCentersAggregateValue> values) {
		
		KCentersAggregateValue kcenters = new KCentersAggregateValue();
		
		Iterator<KCentersAggregateValue> it = values.iterator();
		
		ArrayList<ArrayList<Float>> contents = null;
		
		//Init the contents with the first aggregate value.
		if (it.hasNext()) {
			contents = (ArrayList<ArrayList<Float>>) it.next().getValue().clone();
		}
		
		/*LOG.info("[KCentersAggregator] before aggregate contents: ");
		for (int i = 0; i < contents.size(); i ++) {
			String tmpCenter = "";
			ArrayList<Float> center = contents.get(i);
			for (int j = 0; j < center.size(); j ++) {
				tmpCenter = tmpCenter + " " + center.get(j);
			}
			LOG.info("[KCentersAggregator] [" + tmpCenter + " ]");
		}*/

		//Sum the same class's coordinate values and point's counts into the content.
		while (it.hasNext()) {
			ArrayList<ArrayList<Float>> value = it.next().getValue();
			
/*			if (value.size() > 0) {
				if (contents.size() == 0) {
					contents = value;
				}
			} else {
				continue;
			}*/
			
			//Sum the corresponding element of the array except the first k rows.
			for (int i = 0; i < value.size(); i ++) {
				ArrayList<Float> center = contents.get(i);
				ArrayList<Float> valueCenter = value.get(i);
				for (int j = 0; j < valueCenter.size(); j ++) {
					center.set(j, center.get(j) + valueCenter.get(j));
				}
				contents.set(i, center);
			}
		}
		
		/*LOG.info("[KCentersAggregator] after aggregate contents: ");
		for (int i = 0; i < contents.size(); i ++) {
			String tmpCenter = "";
			ArrayList<Float> center = contents.get(i);
			for (int j = 0; j < center.size(); j ++) {
				tmpCenter = tmpCenter + " " + center.get(j);
			}
			LOG.info("[KCentersAggregator] [" + tmpCenter + " ]");
		}*/
		
		if (contents != null)
			kcenters.setValue(contents);
		
		return kcenters;
	}

}

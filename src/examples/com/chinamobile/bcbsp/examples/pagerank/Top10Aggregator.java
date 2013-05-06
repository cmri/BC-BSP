/**
 * Top10Aggregator.java
 */
package com.chinamobile.bcbsp.examples.pagerank;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.chinamobile.bcbsp.api.Aggregator;

/**
 * Top10Aggregator
 * An example implementation of Aggregator.
 * To get the top 10 pagerank node.
 * 
 * @author Bai Qiushi
 * @version 0.1 2011-12-14
 */
public class Top10Aggregator extends Aggregator<AggregateValueTop10Node> {

    /* (non-Javadoc)
     * @see com.chinamobile.bcbsp.api.AggregatorInterface#aggregate(java.lang.Iterable)
     */
    @Override
    public AggregateValueTop10Node aggregate(
            Iterable<AggregateValueTop10Node> aggValues) {
        
        Map<String, Double> nodeMap = new TreeMap<String, Double>();
        
        Iterator<AggregateValueTop10Node> iter = aggValues.iterator();
        while (iter.hasNext()) {
            String top10Node = iter.next().getValue();
            String[] top10Nodes = top10Node.split("\\|");
            for (int i = 0; i < top10Nodes.length; i ++) {
                String[] node = top10Nodes[i].split("=");
                nodeMap.put(node[0], Double.valueOf(node[1]));
            }
        }
        
        // Put the node map to a list.
        List<Map.Entry<String, Double>> nodes = new ArrayList<Map.Entry<String, Double>>(nodeMap.entrySet());
        // Sort the nodes list by the value.
        Collections.sort(nodes, new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1,
                    Map.Entry<String, Double> o2) {
                return ( int ) (o2.getValue() - o1.getValue());
            }
        });
        
        String aggValue = nodes.get(0).getKey() + "=" + nodes.get(0).getValue();
        for (int i = 1; i < 10; i ++) {
            String id = nodes.get(i).getKey();
            Double pr = nodes.get(i).getValue();
            aggValue = aggValue + "|" + id + "=" + pr;
        }
        
        AggregateValueTop10Node result = new AggregateValueTop10Node();
        
        result.setValue(aggValue);
        
        return result;
    }

}

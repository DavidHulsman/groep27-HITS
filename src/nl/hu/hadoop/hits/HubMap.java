package nl.hu.hadoop.hits;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class HubMap extends Mapper<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    private HashMap<String, Node> nodes;

    @Override
    protected void setup(Context context) {
        nodes = new HashMap<>();
    }

    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // Skip comments
        if (key.toString().startsWith("#")) {
            return;
        }
        // Write the node and it's value + subnodes
        context.write(key, value);
        Node node = Node.fromMR(value.toString());
        nodes.put(key.toString(), node);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            if (entry.getValue().getOutgoingNodeNames() != null && entry.getValue().getOutgoingNodeNames().length == 0) {
                entry.getValue().setHubScore(0);
                outKey.set(entry.getKey());
                outValue.set(entry.getValue().toString());
                context.write(outKey, outValue);
            }

            if (entry.getValue().getOutgoingNodeNames() != null && entry.getValue().getOutgoingNodeNames().length > 0) {
                // Write all the node's subnodes and their new pagerank
                outKey.set(entry.getKey());
                outValue.set(entry.getValue().toString());
                context.write(outKey, outValue);

                for (String neighbor : entry.getValue().getOutgoingNodeNames()) {
                    Node neigh = nodes.get(neighbor);
                    Double score = neigh.getAuthScore();
                    Node outgoingNode = new Node().setHubScore(score);
                    outKey.set(entry.getKey());
                    outValue.set(outgoingNode.toString());
                    context.write(outKey, outValue);
                }
            }
        }
    }
}
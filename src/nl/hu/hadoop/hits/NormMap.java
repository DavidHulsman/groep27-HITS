package nl.hu.hadoop.hits;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Tel alle Auth en Hubscores gekwadrateerd bij elkaar op
class NormMap extends Mapper<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    private double authSquared;
    private double hubSquared;
    private HashMap<String, Node> nodes;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        authSquared = 0;
        hubSquared = 0;
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

        authSquared += node.getAuthScore() * node.getAuthScore();
        hubSquared += node.getHubScore() * node.getHubScore();
        nodes.put(key.toString(), node);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            outKey.set(entry.getKey());
            outValue.set(entry.getValue().toString() + ":"+authSquared+","+hubSquared);
            context.write(outKey, outValue);
        }
    }
}
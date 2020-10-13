package nl.hu.hadoop.hits;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class HubReduce extends Reducer<Text, Text, Text, Text> {
    public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
    public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
    private Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // FIXME for AuthReduce: add hubscores and set as new authScore
        double newHubScore = 0;
        Node originalNode = new Node();
        for (Text textValue : values) {
            Node node = Node.fromMR(textValue.toString());

            if (node.containsOutgoingNodes() || node.containsIncomingNodes()) {
                // the original node
                originalNode = node;
            } else {
                newHubScore += node.getHubScore();
            }
        }
        originalNode.setHubScore(newHubScore);
        outValue.set(originalNode.toString());
        context.write(key, outValue);
    }

    public static enum Counter {
        CONV_DELTAS
    }
}

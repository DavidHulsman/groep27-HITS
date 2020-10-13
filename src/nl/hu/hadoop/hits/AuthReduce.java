package nl.hu.hadoop.hits;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class AuthReduce extends Reducer<Text, Text, Text, Text> {
    public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
    public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
    private Text outValue = new Text();

    @Override
    protected void setup(Context context) {
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // FIXME for AuthReduce: add hubscores and set as new authScore
        double newAuthScore = 0;
        Node originalNode = new Node();
        for (Text textValue : values) {
            Node node = Node.fromMR(textValue.toString());

            if (node.containsOutgoingNodes() || node.containsIncomingNodes()) {
                // the original node
                originalNode = node;
            } else {
                newAuthScore += node.getAuthScore();
            }
        }
        originalNode.setAuthScore(newAuthScore);
        outValue.set(originalNode.toString());
        context.write(key, outValue);
    }
}

package nl.hu.hadoop.hits;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class NormReduce extends Reducer<Text, Text, Text, Text> {
    public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
    public static final double DAMPING_FACTOR = 0.85;
    public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
    private int numberOfNodesInGraph;
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numberOfNodesInGraph = context.getConfiguration().getInt(CONF_NUM_NODES_GRAPH, 0);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text textValue : values) {
            Node node = Node.fromMR(textValue.toString());
            double delta = node.getAuthScore();
            if (node.getAuthSquaredScore() != 0) {
                node.setAuthScore(node.getAuthScore() / Math.sqrt(node.getAuthSquaredScore()));
            } else {
                node.setAuthScore(0);
            }
            delta -= node.getAuthScore();
            if (node.getHubSquaredScore() != 0) {
                node.setHubScore(node.getHubScore() / Math.sqrt(node.getHubSquaredScore()));
            } else {
                node.setHubScore(0);
            }

            outKey.set(key);
            outValue.set(node.toString());

            context.write(outKey, outValue);
            int scaledDelta = Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));
            context.getCounter(Counter.CONV_DELTAS).increment(scaledDelta);
            return;
        }
    }


    public static enum Counter {
        CONV_DELTAS
    }
}

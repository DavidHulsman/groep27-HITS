package nl.hu.hadoop.hits;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;

public class HITS {
    public static void main(String... args) throws Exception {
        iterate("./p2p-Gnutella31.txt", "./output");
    }

    public static void iterate(String input, String output) throws Exception {
        Configuration conf = new Configuration();

        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        outputPath.getFileSystem(conf).mkdirs(outputPath);

        Path inputPath = new Path(outputPath, "input.txt");

        int numNodes = createInputFile(new Path(input), inputPath);

        int iter = 1;
        double desiredConvergence = 0.00001;

        while (true) {
            Path authJobOutputPath = new Path(outputPath, String.valueOf(iter) + "auth");
            Path hubJobOutputPath = new Path(outputPath, String.valueOf(iter) + "hub");
            Path normJobOutputPath = new Path(outputPath, String.valueOf(iter) + "norm");

            System.out.println("======================================");
            System.out.println("=  Iteration:    " + iter);
            System.out.println("=  Input path:   " + inputPath);
            System.out.println("=  Output path:  " + authJobOutputPath);
            System.out.println("======================================");

            calcAuthScore(inputPath, authJobOutputPath, numNodes);
            calcHubScore(authJobOutputPath, hubJobOutputPath, numNodes);

            if (calcNorm(hubJobOutputPath, normJobOutputPath, numNodes) < desiredConvergence) {
                System.out.println("Convergence is below " + desiredConvergence + ", we're done");
                break;
            }
            inputPath = normJobOutputPath;
            iter++;
        }
    }

    public static int createInputFile(Path file, Path targetFile)
            throws IOException {
        Configuration conf = new Configuration();

        FileSystem fs = file.getFileSystem(conf);
        int numNodes = getNumNodes(file);
        double INITIAL_SCORE = 1.0;

        OutputStream os = fs.create(targetFile);
        LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
        HashMap<String, Node> nodes = new HashMap<>();

        while (iter.hasNext()) {
            String line = iter.nextLine();
            if (line.startsWith("#"))
                continue;
            String[] parts = StringUtils.split(line);
            Node outgoingNode = nodes.get(parts[0]);
            if (outgoingNode == null) {
                outgoingNode = new Node()
                        .setAuthScore(INITIAL_SCORE)
                        .setHubScore(INITIAL_SCORE)
                        .addOutgoingNodeNames(parts[1]);
                nodes.put(parts[0], outgoingNode);
            } else {
                outgoingNode.addOutgoingNodeNames(parts[1]);
            }
            Node incomingNode = nodes.get(parts[1]);
            if (incomingNode == null) {
                incomingNode = new Node()
                        .setAuthScore(INITIAL_SCORE)
                        .setHubScore(INITIAL_SCORE)
                        .addIncomingNodeNames(parts[0]);
                nodes.put(parts[1], incomingNode);
            } else {
                incomingNode.addIncomingNodeNames(parts[0]);
            }
        }
        for (java.util.Map.Entry<String, Node> entry : nodes.entrySet()) {
            IOUtils.write(entry.getKey() + '\t' + entry.getValue().toString() + '\n', os);
        }
        os.close();
        return numNodes;
    }

    public static int getNumNodes(Path file) throws IOException {
        Configuration conf = new Configuration();

        FileSystem fs = file.getFileSystem(conf);
        LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");

        HashSet<Integer> nodeIds = new HashSet<>();
        while (iter.hasNext()) {
            String line = iter.nextLine();
            if (line.startsWith("#"))
                continue;
            String[] parts = StringUtils.split(line);
            nodeIds.add(Integer.parseInt(parts[0]));
            nodeIds.add(Integer.parseInt(parts[1]));
        }
        return nodeIds.size();
    }

    public static void calcAuthScore(Path inputPath, Path outputPath, int numNodes)
            throws Exception {
        Configuration conf = new Configuration();

        conf.setInt(AuthReduce.CONF_NUM_NODES_GRAPH, numNodes);

        Job job = Job.getInstance(conf, "calcAuthScore");
        job.setJarByClass(HITS.class);
        job.setMapperClass(AuthMap.class);
        job.setReducerClass(AuthReduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }
    }

    public static void calcHubScore(Path inputPath, Path outputPath, int numNodes)
            throws Exception {
        Configuration conf = new Configuration();

        conf.setInt(HubReduce.CONF_NUM_NODES_GRAPH, numNodes);

        Job job = Job.getInstance(conf, "calcHubScore");
        job.setJarByClass(HITS.class);
        job.setMapperClass(HubMap.class);
        job.setReducerClass(HubReduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }
    }

    public static double calcNorm(Path inputPath, Path outputPath, int numNodes)
            throws Exception {
        Configuration conf = new Configuration();

        conf.setInt(NormReduce.CONF_NUM_NODES_GRAPH, numNodes);

        Job job = Job.getInstance(conf, "calcNorm");
        job.setJarByClass(HITS.class);
        job.setMapperClass(NormMap.class);
        job.setReducerClass(NormReduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }

        long summedConvergence = job.getCounters().findCounter(
                NormReduce.Counter.CONV_DELTAS).getValue();
        double convergence =
                ((double) summedConvergence /
                        NormReduce.CONVERGENCE_SCALING_FACTOR) /
                        (double) numNodes;

        System.out.println("======================================");
        System.out.println("=  Num nodes:           " + numNodes);
        System.out.println("=  Summed convergence:  " + summedConvergence);
        System.out.println("=  Convergence:         " + convergence);
        System.out.println("======================================");

        return convergence;
    }
}


package nl.hu.hadoop.hits;


import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Node {
    public static final char nodeFieldSeparator = ',';
    public static final char fieldSeparator = '\t';
    public static final String squaredSeparator = ":";
    private double authScore = 0.0;
    private double hubScore = 0.0;
    private double authSquaredScore = 0.0;
    private double hubSquaredScore = 0.0;
    private Set<String> outgoingNodeNames = new HashSet<>();
    private Set<String> incomingNodeNames = new HashSet<>();

    public static Node fromMR(String value) throws IOException {
        /// WARNING: HORRIBLE HACKY CODE!
        Double localAuthScore = null;
        Double localHubScore = null;
        // if the value contains :n,m at the end
        if (value.contains(squaredSeparator)) {
            List<String> parts = Arrays.asList(StringUtils.splitPreserveAllTokens(
                    value, squaredSeparator));
            value = parts.get(0);
            List<String> values = Arrays.asList(StringUtils.splitPreserveAllTokens(
                    parts.get(1), nodeFieldSeparator));
            localAuthScore = Double.parseDouble(values.get(0));
            localHubScore = Double.parseDouble(values.get(1));
        }
        /// WARNING: END OF HORRIBLE HACKY CODE! (hopefully)

        List<String> parts = Arrays.asList(StringUtils.splitPreserveAllTokens(
                value, fieldSeparator));
        if (parts.size() < 1) {
            throw new IOException(
                    "Expected 1 or more parts but received " + parts.size());
        }
        Node node = new Node()
                .setAuthScore(Double.valueOf(parts.get(0)))
                .setHubScore(Double.valueOf(parts.get(1)));
        /// more code connected to the ewy code above
        if (localAuthScore != null) {
            node.setAuthSquaredScore(localAuthScore);
            node.setHubSquaredScore(localHubScore);
        }
        if (parts.size() > 2) {
            List<String> incomingNodes = Arrays.asList(StringUtils.splitPreserveAllTokens(
                    parts.get(2), nodeFieldSeparator));

            for (String neighbor : incomingNodes) {
                if (!neighbor.equals("")) {
                    node.addIncomingNodeNames(neighbor);
                }
            }

            List<String> outgoingNodes = Arrays.asList(StringUtils.splitPreserveAllTokens(
                    parts.get(3), nodeFieldSeparator));

            for (String neighbor : outgoingNodes) {
                if (!neighbor.equals("")) {
                    node.addOutgoingNodeNames(neighbor);
                }
            }
        }
        return node;
    }

    public double getAuthScore() {
        return authScore;
    }

    public Node setAuthScore(double authScore) {
        this.authScore = authScore;
        return this;
    }

    public double getHubScore() {
        return hubScore;
    }

    public Node setHubScore(double hubScore) {
        this.hubScore = hubScore;
        return this;
    }

    public String[] getOutgoingNodeNames() {
        return outgoingNodeNames.toArray(new String[outgoingNodeNames.size()]);
    }

    public String[] getIncomingNodeNames() {
        return incomingNodeNames.toArray(new String[incomingNodeNames.size()]);
    }

    public boolean containsOutgoingNodes() {
        return outgoingNodeNames.size() > 0;
    }

    public boolean containsIncomingNodes() {
        return incomingNodeNames.size() > 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(authScore);
        sb.append(fieldSeparator);
        sb.append(hubScore);

        if (getIncomingNodeNames() != null) {
            sb.append(fieldSeparator)
                    .append(StringUtils
                            .join(getIncomingNodeNames(), nodeFieldSeparator));
        }

        if (getOutgoingNodeNames() != null) {
            sb.append(fieldSeparator)
                    .append(StringUtils
                            .join(getOutgoingNodeNames(), nodeFieldSeparator));
        }
        return sb.toString();
    }

    public Node addOutgoingNodeNames(String part) {
        outgoingNodeNames.add(part);
        return this;
    }

    public Node addIncomingNodeNames(String part) {
        incomingNodeNames.add(part);
        return this;
    }

    public double getAuthSquaredScore() {
        return authSquaredScore;
    }

    public void setAuthSquaredScore(double authSquaredScore) {
        this.authSquaredScore = authSquaredScore;
    }

    public double getHubSquaredScore() {
        return hubSquaredScore;
    }

    public void setHubSquaredScore(double hubSquaredScore) {
        this.hubSquaredScore = hubSquaredScore;
    }
}
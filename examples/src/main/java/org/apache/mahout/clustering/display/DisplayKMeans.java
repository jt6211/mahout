/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.clustering.display;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.ClusterClassifier;
import org.apache.mahout.clustering.iterator.ClusterIterator;
import org.apache.mahout.clustering.iterator.KMeansClusteringPolicy;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.Vector;

import com.google.common.collect.Lists;

public class DisplayKMeans extends DisplayClustering {
  
  DisplayKMeans() {
    initialize();
    this.setTitle("k-Means Clusters (>" + (int) (significance * 100) + "% of population)");
  }
  
  public static void main(String[] args) throws Exception {
    DistanceMeasure measure = new ManhattanDistanceMeasure();
    Path samples = new Path("samples");
    Path output = new Path("output");
    Configuration conf = new Configuration();
    HadoopUtil.delete(conf, samples);
    HadoopUtil.delete(conf, output);
    
    RandomUtils.useTestSeed();
    DisplayClustering.generateSamples();
    writeSampleData(samples);
    boolean runClusterer = false;
    double convergenceDelta = 0.001;
    if (runClusterer) {
      int numClusters = 3;
      runSequentialKMeansClusterer(conf, samples, output, measure, numClusters, convergenceDelta);
    } else {
      int maxIterations = 10;
      runSequentialKMeansClassifier(conf, samples, output, measure, maxIterations, convergenceDelta);
    }
    new DisplayKMeans();
  }
  
  private static void runSequentialKMeansClassifier(Configuration conf, Path samples, Path output,
      DistanceMeasure measure, int numClusters, double convergenceDelta) throws IOException {
    Collection<Vector> points = Lists.newArrayList();
    for (int i = 0; i < numClusters; i++) {
      points.add(SAMPLE_DATA.get(i).get());
    }
    List<Cluster> initialClusters = Lists.newArrayList();
    int id = 0;
    for (Vector point : points) {
      initialClusters.add(new org.apache.mahout.clustering.kmeans.Kluster(point, id++, measure));
    }
    ClusterClassifier prior = new ClusterClassifier(initialClusters, new KMeansClusteringPolicy(convergenceDelta));
    Path priorPath = new Path(output, "clusters-0");
    prior.writeToSeqFiles(priorPath);
    
    int maxIter = 10;
    new ClusterIterator().iterateSeq(samples, priorPath, output, maxIter);
    loadClustersWritable(output);
  }
  
  private static void runSequentialKMeansClusterer(Configuration conf, Path samples, Path output,
      DistanceMeasure measure, int maxIterations, double convergenceDelta) throws IOException, InterruptedException,
      ClassNotFoundException {
    Path clusters = RandomSeedGenerator.buildRandom(conf, samples, new Path(output, "clusters-0"), 3, measure);
    KMeansDriver.run(samples, clusters, output, measure, convergenceDelta, maxIterations, true, true);
    loadClusters(output);
  }
  
  // Override the paint() method
  @Override
  public void paint(Graphics g) {
    plotSampleData((Graphics2D) g);
    plotClusters((Graphics2D) g);
  }
}

/**
 * Copyright 2013-2015 Pierre Merienne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.pmerienne.trident.ml.clustering;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.pmerienne.trident.ml.clustering.ClusterQuery;
import com.github.pmerienne.trident.ml.clustering.ClusterUpdater;
import com.github.pmerienne.trident.ml.clustering.KMeans;
import com.github.pmerienne.trident.ml.core.Instance;
import com.github.pmerienne.trident.ml.preprocessing.InstanceCreator;
import com.github.pmerienne.trident.ml.testing.RandomFeaturesForClusteringSpout;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ClustererTridentIntegrationTest {

	@Test
	public void testInTopology() throws InterruptedException {
		// Start local cluster
		LocalCluster cluster = new LocalCluster();
		LocalDRPC localDRPC = new LocalDRPC();

		try {
			// Build topology
			TridentTopology toppology = new TridentTopology();

			// Training stream
			TridentState kmeansState = toppology
				// Emit tuples with a instance containing an integer as label and 3
				// double features named (x0, x1 and x2)
				.newStream("samples", new RandomFeaturesForClusteringSpout())

				// Convert trident tuple to instance
				.each(new Fields("label", "x0", "x1", "x2"), new InstanceCreator<Integer>(), new Fields("instance"))

				// Update a 3 classes kmeans
				.partitionPersist(new MemoryMapState.Factory(), new Fields("instance"), new ClusterUpdater("kmeans", new KMeans(3)));

			// Cluster stream
			toppology.newDRPCStream("predict", localDRPC)
				// Convert DRPC args to instance
				.each(new Fields("args"), new DRPCArgsToInstance(), new Fields("instance"))

				// Query kmeans to classify instance
				.stateQuery(kmeansState, new Fields("instance"), new ClusterQuery("kmeans"), new Fields("prediction"))
					
				.project(new Fields("prediction"));

			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), toppology.build());

			Thread.sleep(10000);

			Integer result11 = extractPrediction(localDRPC.execute("predict", "1.0 1.0 1.0"));
			Integer result12 = extractPrediction(localDRPC.execute("predict", "0.8 1.1 0.9"));
			assertEquals(result11, result12);

			Integer result21 = extractPrediction(localDRPC.execute("predict", "1.0 -1.0 1.0"));
			Integer result22 = extractPrediction(localDRPC.execute("predict", "0.8 -1.1 0.9"));
			assertEquals(result21, result22);

			Integer result31 = extractPrediction(localDRPC.execute("predict", "1.0 -1.0 -1.0"));
			Integer result32 = extractPrediction(localDRPC.execute("predict", "0.8 -1.1 -0.9"));
			assertEquals(result31, result32);
		} finally {
			cluster.shutdown();
			localDRPC.shutdown();
		}
	}

	protected static Integer extractPrediction(String drpcResult) {
		return Integer.parseInt(drpcResult.replaceAll("\\[", "").replaceAll("\\]", ""));
	}

	public static class DRPCArgsToInstance extends BaseFunction {

		private static final long serialVersionUID = -2932222000448806586L;

		@SuppressWarnings("rawtypes")
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String[] args = tuple.getString(0).split(" ");
			double[] features = new double[args.length];
			for (int i = 0; i < args.length; i++) {
				features[i] = Double.parseDouble(args[i]);
			}

			Instance<?> instance = new Instance(features);
			collector.emit(new Values(instance));
		}
	}

}

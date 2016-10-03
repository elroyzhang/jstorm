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
package com.github.pmerienne.trident.ml.nlp;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import storm.trident.TridentTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.tuple.Fields;

public class TwitterSentimentClassifierTridentIntegrationTest {

	@Test
	public void testInTopology() throws InterruptedException {
		// Start local cluster
		LocalCluster cluster = new LocalCluster();
		LocalDRPC localDRPC = new LocalDRPC();

		try {
			// Build topology
			TridentTopology toppology = new TridentTopology();

			// Classification stream
			toppology.newDRPCStream("classify", localDRPC)
				// Query classifier with text instance
				.each(new Fields("args"), new TwitterSentimentClassifier(), new Fields("sentiment")).project(new Fields("sentiment"));

			cluster.submitTopology(this.getClass().getSimpleName(), new Config(), toppology.build());
			Thread.sleep(4000);

			// Query with DRPC
			test(false, "RT @JazminBianca: I hate Windows 8. I hate Windows 8. I hate Windows 8.", localDRPC);
			test(false, "I don't like Windows 8, I think it's overrated =))", localDRPC);
			test(false, "Windows 8 is stupid as fuck ! Shit is confusing <<", localDRPC);
			test(false, "not a big fan of Windows 8", localDRPC);
			test(false, "Forever hating apple for changing the chargers #wanks", localDRPC);
			test(false, "#CSRBlast #CSRBlast That moment you pull out a book because the customer service at apple is horrible and takes wa... http://t.co/WxqyGR9a85", localDRPC);

			test(true, "Windows 8 is awesome :D", localDRPC);
			test(true, "God Windows 8 is amazing. Finally", localDRPC);
			test(true, "Register for the AWESOME Windows 8 western US regional events all in the next few weeks! http://t.co/7lfqaHSxfs #w8appfactor @w8appfactor", localDRPC);
			test(true, "Windows 8 is fun to use. I like it better then mac lion.", localDRPC);
			test(true, "Good morning loves 😁😁 apple jacks doe http://t.co/nOfi42enoQ", localDRPC);
			test(true, "@Saad_khan33 No i prefer apple anyday", localDRPC);

		} finally {
			cluster.shutdown();
			localDRPC.shutdown();
		}
	}

	protected static void test(boolean expected, String text, LocalDRPC localDRPC) {
		boolean actual = extractPrediction(localDRPC.execute("classify", text));
		assertEquals("Expecting " + expected + " but was " + actual + " for " + text, expected, actual);
	}

	protected static Boolean extractPrediction(String drpcResult) {
		return Boolean.parseBoolean(drpcResult.replaceAll("\\[", "").replaceAll("\\]", ""));
	}
}

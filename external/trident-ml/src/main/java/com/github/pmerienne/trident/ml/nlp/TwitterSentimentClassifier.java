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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.github.pmerienne.trident.ml.classification.Classifier;
import com.github.pmerienne.trident.ml.classification.PAClassifier;
import com.github.pmerienne.trident.ml.core.TextInstance;
import com.github.pmerienne.trident.ml.preprocessing.TwitterTokenizer;
import com.github.pmerienne.trident.ml.testing.data.Datasets;

public class TwitterSentimentClassifier extends BaseFunction implements Serializable {

	private static final long serialVersionUID = 1553274753609262633L;

	protected TextFeaturesExtractor featuresExtractor;
	protected Classifier<Boolean> classifier;

	private TwitterTokenizer tokenizer = new TwitterTokenizer(2, 2);

	public TwitterSentimentClassifier() {
		try {
			this.featuresExtractor = Builder.loadFeatureExtractor();
			this.classifier = Builder.loadClassifier();
		} catch (IOException e) {
			throw new RuntimeException("Unable to load TwitterSentimentClassifier : " + e.getMessage(), e);
		}
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String text = tuple.getString(0);
		boolean prediction = this.classify(text);
		collector.emit(new Values(prediction));
	}

	protected Boolean classify(String text) {
		List<String> tokens = this.tokenizer.tokenize(text);
		double[] features = this.featuresExtractor.extractFeatures(tokens);
		Boolean prediction = this.classifier.classify(features);
		return prediction;
	}

	protected static class Builder {

		private final static File TEXT_FEATURES_EXTRACTOR_FILE = new File(Builder.class.getResource("/twitter-sentiment-classifier-extractor.json").getFile());
		private final static File CLASSIFIER_FILE = new File(Builder.class.getResource("/twitter-sentiment-classifier-classifier.json").getFile());

		private final static ObjectMapper MAPPER = new ObjectMapper();

		public static void main(String[] args) throws IOException {
			// Get some tweets
			List<TextInstance<Boolean>> dataset = Datasets.getTwitterSamples();
			List<List<String>> documents = new ArrayList<List<String>>();
			for (TextInstance<Boolean> instance : dataset) {
				documents.add(instance.tokens);
			}

			// Init feature extractor
			TFIDF featuresExtractor = new TFIDF(documents, 10000);

			// Init and train classifier
			PAClassifier classifier = new PAClassifier();
			double[] features;
			for (TextInstance<Boolean> instance : dataset) {
				features = featuresExtractor.extractFeatures(instance.tokens);
				classifier.update(instance.label, features);
			}

			// save them
			save(featuresExtractor, classifier);
		}

		protected static void save(TFIDF featuresExtractor, PAClassifier classifier) throws IOException {
			MAPPER.writeValue(TEXT_FEATURES_EXTRACTOR_FILE, featuresExtractor);
			MAPPER.writeValue(CLASSIFIER_FILE, classifier);
		}

		public static TextFeaturesExtractor loadFeatureExtractor() throws IOException {
			return MAPPER.readValue(TEXT_FEATURES_EXTRACTOR_FILE, TFIDF.class);
		}

		public static Classifier<Boolean> loadClassifier() throws IOException {
			return MAPPER.readValue(CLASSIFIER_FILE, PAClassifier.class);
		}
	}
}

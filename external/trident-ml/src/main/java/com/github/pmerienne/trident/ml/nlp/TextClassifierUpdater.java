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

import java.util.Arrays;
import java.util.List;

import com.github.pmerienne.trident.ml.core.TextInstance;
import com.github.pmerienne.trident.ml.util.KeysUtil;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

public class TextClassifierUpdater<L> extends BaseStateUpdater<MapState<TextClassifier<L>>> {

	private static final long serialVersionUID = 1943890181994862536L;

	private String classifierName;
	private TextClassifier<L> initialClassifier;

	public TextClassifierUpdater(String classifierName, TextClassifier<L> initialClassifier) {
		this.classifierName = classifierName;
		this.initialClassifier = initialClassifier;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void updateState(MapState<TextClassifier<L>> state, List<TridentTuple> tuples, TridentCollector collector) {
		// Get model
		List<TextClassifier<L>> classifiers = state.multiGet(KeysUtil.toKeys(this.classifierName));
		TextClassifier<L> classifier = null;
		if (classifiers != null && !classifiers.isEmpty()) {
			classifier = classifiers.get(0);
		}

		// Init it if necessary
		if (classifier == null) {
			classifier = this.initialClassifier;
		}

		// Update model
		TextInstance<L> instance;
		for (TridentTuple tuple : tuples) {
			instance = (TextInstance<L>) tuple.get(0);
			classifier.update(instance.label, instance.tokens);
		}

		// Save model
		state.multiPut(KeysUtil.toKeys(this.classifierName), Arrays.asList(classifier));
	}

}

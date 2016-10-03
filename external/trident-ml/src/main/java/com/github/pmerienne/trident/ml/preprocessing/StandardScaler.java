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
package com.github.pmerienne.trident.ml.preprocessing;

import com.github.pmerienne.trident.ml.core.Instance;
import com.github.pmerienne.trident.ml.stats.StreamFeatureStatistics;
import com.github.pmerienne.trident.ml.stats.StreamStatistics;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class StandardScaler extends BaseFunction {

	private static final long serialVersionUID = 1740717206768121351L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Instance<?> instance = (Instance<?>) tuple.get(0);
		StreamStatistics streamStatistics = (StreamStatistics) tuple.get(1);

		Instance<?> standardizedInstance = this.standardize(instance, streamStatistics);
		collector.emit(new Values(standardizedInstance));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Instance<?> standardize(Instance<?> instance, StreamStatistics streamStatistics) {
		// init new features
		int featuresSize = instance.features.length;
		double[] standardizedFeatures = new double[featuresSize];

		// Standardize each feature
		StreamFeatureStatistics featureStatistics;
		for (int i = 0; i < featuresSize; i++) {
			featureStatistics = streamStatistics.getFeaturesStatistics().get(i);
			standardizedFeatures[i] = (instance.features[i] - featureStatistics.getMean()) / featureStatistics.getStdDev();
		}

		Instance<?> standardizedInstance = new Instance(instance.label, standardizedFeatures);
		return standardizedInstance;
	}
}

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
package com.github.pmerienne.trident.ml.stats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class StreamStatistics implements Serializable {

	private static final long serialVersionUID = -3873210308112567893L;

	private Type type = Type.FIXED;
	private Long adativeMaxSize = 1000L;

	private List<StreamFeatureStatistics> featuresStatistics = new ArrayList<StreamFeatureStatistics>();

	public StreamStatistics() {
	}

	public StreamStatistics(Type type) {
		this.type = type;
	}

	public StreamStatistics(Type type, Long adativeMaxSize) {
		this.type = type;
		this.adativeMaxSize = adativeMaxSize;
	}

	public void update(double[] features) {
		StreamFeatureStatistics featureStatistics;
		for (int i = 0; i < features.length; i++) {
			featureStatistics = this.getStreamStatistics(i);
			featureStatistics.update(features[i]);
		}
	}

	private StreamFeatureStatistics getStreamStatistics(int index) {
		if (this.featuresStatistics.size() < index + 1) {
			StreamFeatureStatistics featureStatistics = this.createFeatureStatistics();
			this.featuresStatistics.add(featureStatistics);
		}
		return this.featuresStatistics.get(index);
	}

	private StreamFeatureStatistics createFeatureStatistics() {
		StreamFeatureStatistics featureStatistics = null;
		switch (this.type) {
		case FIXED:
			featureStatistics = new FixedStreamFeatureStatistics();
			break;
		case ADAPTIVE:
			featureStatistics = new AdaptiveStreamFeatureStatistics(this.adativeMaxSize);
			break;
		default:
			break;
		}
		return featureStatistics;
	}

	public static StreamStatistics fixed() {
		return new StreamStatistics();
	}

	public static StreamStatistics adaptive(Long maxSize) {
		return new StreamStatistics(Type.ADAPTIVE, maxSize);
	}

	public List<StreamFeatureStatistics> getFeaturesStatistics() {
		return featuresStatistics;
	}

	@Override
	public String toString() {
		return "StreamStatistics [type=" + type + ", featuresStatistics=" + featuresStatistics + "]";
	}

	public static enum Type {
		FIXED, ADAPTIVE;
	}
}

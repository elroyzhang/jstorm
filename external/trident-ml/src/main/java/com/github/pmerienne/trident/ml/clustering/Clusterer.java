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

import java.util.List;

public interface Clusterer {

	/**
	 * Classifies a given sample and updates clusters.
	 * 
	 * @param features
	 * @return
	 */
	Integer classify(double[] features);

	/**
	 * Updates clusters with a given sample and return classification.
	 * 
	 * @param features
	 * @return
	 */
	Integer update(double[] features);

	/**
	 * Predicts the cluster memberships for a given instance.
	 * 
	 * @param features
	 * @return
	 */
	double[] distribution(double[] features);

	/**
	 * Returns learned clusters as a {@link List} of feature's means
	 * 
	 * @return
	 */
	double[][] getCentroids();
	
	void reset();
}

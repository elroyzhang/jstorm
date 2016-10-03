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
package com.github.pmerienne.trident.ml.evaluation;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.snapshot.ReadOnlySnapshottable;
import storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class EvaluationQuery<L> extends BaseQueryFunction<ReadOnlySnapshottable<Evaluation<L>>, Double> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<Double> batchRetrieve(ReadOnlySnapshottable<Evaluation<L>> state, List<TridentTuple> args) {
		List<Double> ret = new ArrayList<Double>(args.size());
		
		Evaluation<L> snapshot = state.get();
		for (int i = 0; i < args.size(); i++) {
			ret.add(snapshot.getEvaluation());
		}
		return ret;
	}

	@Override
	public void execute(TridentTuple tuple, Double result, TridentCollector collector) {
		collector.emit(new Values(result));
	}
}

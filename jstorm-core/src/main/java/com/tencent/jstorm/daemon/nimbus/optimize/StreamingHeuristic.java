/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.jstorm.daemon.nimbus.optimize;

public class StreamingHeuristic {
  private int[] inputStream;
  private int[][] adjacency;
  private int[] partitionResults;
  private int vertexCount;
  private int partitionCount;
  private int[] partitionSlot;
  private int[] partitionVertexCount;
  private int MaxVolume;

  private void InitPartitionSlot() {
    int factor = (int) ((float) this.vertexCount / (float) this.partitionCount);
    int remainder = this.vertexCount - this.partitionCount * factor;
    this.MaxVolume = factor + 2;
    for (int i = 0; i < this.partitionCount; i++) {
      this.partitionSlot[i] = factor;
    }
    for (int i = 0; i < remainder; i++) {
      this.partitionSlot[i]++;
    }
  }

  public StreamingHeuristic(int nvertexCount, int partitionCount, int[] stream,
      int[][] adjacency) {
    this.vertexCount = nvertexCount;
    this.partitionCount = partitionCount;
    this.partitionSlot = new int[this.partitionCount];
    this.partitionVertexCount = new int[this.partitionCount];
    this.adjacency = new int[this.vertexCount][this.vertexCount];
    this.partitionResults = new int[this.vertexCount];
    this.inputStream = new int[this.vertexCount];

    for (int i = 0; i < this.vertexCount; i++) {
      this.inputStream[i] = stream[i];
      this.partitionResults[i] = -1;
      for (int j = 0; j < this.vertexCount; j++) {
        this.adjacency[i][j] = adjacency[i][j];
      }
    }
    InitPartitionSlot();
  }

  public void Partition() {
    int assignment = 0;
    while (true) {
      if (assignment >= this.vertexCount) {
        break;
      }
      int maxIncome = -1;
      int partitionId = 0;
      int partitionVolume = this.MaxVolume;
      int vertex = this.inputStream[assignment];
      for (int i = 0; i < this.partitionCount; i++) {
        if (this.partitionVertexCount[i] >= this.partitionSlot[i]) {
          continue;
        }
        int incomeCount = 0;
        for (int j = 0; j < this.vertexCount; j++) {
          if (this.partitionResults[j] == i && this.adjacency[j][vertex] == 1) {
            incomeCount++;
          }
        }
        if (incomeCount >= maxIncome
            && partitionVertexCount[i] < partitionVolume) {
          maxIncome = incomeCount;
          partitionId = i;
          partitionVolume = partitionVertexCount[i];
        }
      }
      this.partitionResults[vertex] = partitionId;
      this.partitionVertexCount[partitionId]++;
      assignment++;
    }
  }

  public void TestOuput() {
    for (int i = 0; i < this.vertexCount; i++) {
      System.out.print(this.partitionResults[i] + " ");
    }
  }
}

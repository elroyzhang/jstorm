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

public class TopologySorting {
  private int[][] inputTopology;
  private int[] outputList;
  private int[] outputFlag;
  private int vertexCount;

  public TopologySorting(int vertexCount) {
    this.vertexCount = vertexCount;
    this.inputTopology = new int[this.vertexCount][this.vertexCount];
    this.outputList = new int[this.vertexCount];
    this.outputFlag = new int[this.vertexCount];
  }

  public TopologySorting(int vertexCount, int[][] adjacency) {
    this.vertexCount = vertexCount;
    this.inputTopology = new int[this.vertexCount][this.vertexCount];
    this.outputList = new int[this.vertexCount];
    this.outputFlag = new int[this.vertexCount];
    for (int i = 0; i < this.vertexCount; i++) {
      for (int j = 0; j < this.vertexCount; j++) {
        this.inputTopology[i][j] = adjacency[i][j];
      }
    }
  }

  public void AddEdge(int s, int e) {
    this.inputTopology[s][e] = 1;
    this.inputTopology[e][s] = -1;
  }

  public void RemoveEdge(int s, int e) {
    this.inputTopology[s][e] = 0;
    this.inputTopology[e][s] = 0;
  }

  public int[] Sort() {
    this.outputList = new int[this.vertexCount];
    this.outputFlag = new int[this.vertexCount];
    int assignment = 0;
    while (true) {
      if (assignment >= this.vertexCount) {
        break;
      }
      for (int i = 0; i < this.vertexCount; i++) {
        if (this.outputFlag[i] == 1) {
          continue;
        }
        boolean hasNoIndegree = true;
        for (int j = 0; j < this.vertexCount; j++) {
          if (inputTopology[i][j] == -1) {
            hasNoIndegree = false;
            break;
          }
        }
        if (hasNoIndegree) {
          this.outputList[assignment] = i;
          this.outputFlag[i] = 1;
          assignment++;
          for (int k = 0; k < this.vertexCount; k++) {
            if (inputTopology[i][k] == 1) {
              RemoveEdge(i, k);
            }
          }
          break;
        }
      }
    }
    return this.outputList;
  }

  public int[] OutputSortedStream() {
    return this.outputList;
  }

  public void TestOutput() {
    for (int i = 0; i < this.vertexCount; i++) {
      for (int j = 0; j < vertexCount; j++) {
        System.out.print(this.inputTopology[i][j]);
      }
      System.out.println();
    }
    for (int i = 0; i < this.vertexCount; i++) {
      System.out.print(outputList[i] + " ");
    }
  }
}

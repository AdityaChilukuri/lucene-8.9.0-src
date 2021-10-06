package perf;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.queryparser.classic.ParseException;

// Serves up tasks from locally loaded list:
class LocalTaskSource implements TaskSource {
  private final List<Task> tasks;
  private final AtomicInteger nextTask = new AtomicInteger();
  
  private final int taskRepeatCount;
  private final int tasksInFileCount;
  private final String logFile;
  private final String perfCommand;

  private Process perfProcess;
  public LocalTaskSource(IndexState indexState, TaskParser taskParser, String tasksFile,
                         Random staticRandom, Random random, int numTaskPerCat, int taskRepeatCount,
                         boolean doPKLookup, boolean groupByCat, String logFile, String perfCommand) throws IOException, ParseException {

    final List<Task> loadedTasks = loadTasks(taskParser, tasksFile);
    Collections.shuffle(loadedTasks, staticRandom);
    final List<Task> prunedTasks = loadedTasks; // pruneTasks(loadedTasks, numTaskPerCat); NO PRUNING! -- Adi
    
    this.taskRepeatCount = taskRepeatCount;
    this.tasksInFileCount = loadedTasks.size();
    this.logFile = logFile;
    this.perfCommand = perfCommand.equals("0") ? null : perfCommand;
    final IndexSearcher searcher = indexState.mgr.acquire();
    final int maxDoc;
    try {
      maxDoc = searcher.getIndexReader().maxDoc();
    } finally {
      indexState.mgr.release(searcher);
    }

    // Add PK tasks
    //System.out.println("WARNING: skip PK tasks");
    if (doPKLookup) {
      final int numPKTasks = (int) Math.min(maxDoc/6000., numTaskPerCat);
      final Set<BytesRef> pkSeenIDs = new HashSet<BytesRef>();
      final Set<Integer> pkSeenIntIDs = new HashSet<Integer>();
      for(int idx=0;idx<numPKTasks;idx++) {
        prunedTasks.add(new PKLookupTask(maxDoc, staticRandom, 4000, pkSeenIDs, idx));
        //prunedTasks.add(new PointsPKLookupTask(maxDoc, staticRandom, 4000, pkSeenIntIDs, idx));
      }
      /*
      final Set<BytesRef> pkSeenSingleIDs = new HashSet<BytesRef>();
      for(int idx=0;idx<numPKTasks*100;idx++) {
        prunedTasks.add(new SinglePKLookupTask(maxDoc, staticRandom, pkSeenSingleIDs, idx));
      }
      */
    }
    tasks = new ArrayList<>();
    if (groupByCat) {
      repeatTasksGrouped(prunedTasks, taskRepeatCount, random);
    } else {
      repeatTasksShuffled(prunedTasks, taskRepeatCount, random);
    }
    System.out.println("TASK LEN=" + tasks.size());
  }

  private void repeatTasksShuffled(List<Task> someTasks, int taskRepeatCount, Random random) {
    // Copy the pruned tasks multiple times, shuffling the order each time:
    for(int iter = 0; iter < taskRepeatCount; iter++) {
      Collections.shuffle(someTasks, random);
      for(Task task : someTasks) {
        tasks.add(task.clone());
      }
    }
  }

  private void repeatTasksGrouped(List<Task> someTasks, int taskRepeatCount, Random random) {
    Map<String, List<Task>> tasksByCategory = new HashMap<>();
    for (Task task : someTasks) {
      String category = task.getCategory();
      tasksByCategory.computeIfAbsent(category, c -> new ArrayList<>()).add(task);
    }
    for (String category : tasksByCategory.keySet()) {
      List<Task> categoryTasks = tasksByCategory.get(category);
      repeatTasksShuffled(categoryTasks, taskRepeatCount, random);
    }
  }

  @Override
  public List<Task> getAllTasks() {
    return tasks;
  }

  private static List<Task> pruneTasks(List<Task> tasks, int numTaskPerCat) {
    final Map<String,Integer> catCounts = new HashMap<String,Integer>();
    final List<Task> newTasks = new ArrayList<Task>();
    for(Task task : tasks) {
      final String cat = task.getCategory();
      Integer v = catCounts.get(cat);
      int catCount;
      if (v == null) {
        catCount = 0;
      } else {
        catCount = v.intValue();
      }

      if (catCount >= numTaskPerCat) {
        // System.out.println("skip task cat=" + cat);
        continue;
      }
      catCount++;
      catCounts.put(cat, catCount);
      newTasks.add(task);
    }

    return newTasks;
  }

  @Override
  public Task nextTask() {
    final int next = nextTask.getAndIncrement();
    if (perfCommand != null && (next % this.tasksInFileCount) == 0 && next != 0) {
        // kill the previous Perf process
        this.perfProcess.destroy();
        try {
          var waitStartTime = System.nanoTime();
          this.perfProcess.waitFor();
          var waitFinishTime = System.nanoTime();
          System.out.println("Run " + next / this.tasksInFileCount + 
                        " done -- Perf process kill time = " + 
                        (waitFinishTime - waitStartTime) / 1000000 + "ms");
        } catch (InterruptedException ie) {
          System.out.println("Interrupted Exception in pid = " + ProcessHandle.current().pid());
        }
    }
    if (next >= tasks.size()) {
      return null;
    }
    // Possibly start / end perf script
    if (perfCommand != null && (next % this.tasksInFileCount) == 0) {
      int count = next / this.tasksInFileCount;
      System.out.println("Run: " + count + "; next: " + next);
        try {
            this.perfProcess = new ProcessBuilder().redirectOutput(new File(this.logFile + ".perf,run=" + count))
                                                   .redirectError(new File(this.logFile + ".perf,run=" + (count) + ".stderr"))
                                                   .command("sh", this.perfCommand, ""+ProcessHandle.current().pid())
                                                   .start();
        } catch(IOException e) {
            // In our case we definitely want perf to work fully
            e.printStackTrace();
            System.exit(-1);
        }
    }

    return tasks.get(next);
  }

  @Override
  public void taskDone(Task task, long queueTimeNS, TotalHits toalHitCount) {
  }

  static List<Task> loadTasks(TaskParser taskParser, String filePath) throws IOException, ParseException {
    final List<Task> tasks = new ArrayList<Task>();
    final BufferedReader taskFile = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"), 16384);
    while (true) {
      String line = taskFile.readLine();
      if (line == null) {
        break;
      }
      line = line.trim();
      if (line.indexOf("#") == 0) {
        // Ignore comment lines
        continue;
      }
      if (line.length() == 0) {
        // Ignore blank lines
        continue;
      }

      tasks.add(taskParser.parseOneTask(line));
    }
    taskFile.close();
    return tasks;
  }
  
}

#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  int num_threads;
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSpinning(int num_threads);
  ~TaskSystemParallelThreadPoolSpinning();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  int num_threads;
  std::vector<std::thread> threads;
  std::mutex mutex;
  std::condition_variable work_cv;
  bool should_exit = false;
  bool has_work = false;
  IRunnable *runnable = nullptr;
  int num_total_tasks = 0;
  int next_task = 0;
  int tasks_completed = 0;

  void workerThreadLoop();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
private:
  int num_threads;
  std::vector<std::thread> threads;
  std::mutex mutex;
  std::condition_variable work_cv;
  std::condition_variable completion_cv;
  bool should_exit;
  bool has_work;
  IRunnable *runnable;
  int num_total_tasks;
  int next_task;
  int tasks_completed;
  void workerThreadLoop();

public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

#endif

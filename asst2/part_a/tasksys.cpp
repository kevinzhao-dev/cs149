#include "tasksys.h"
#include <thread>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemSerial::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads) {
  // Store the number of threads as a member variable for later use
  this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
  std::vector<std::thread> threads;

  // Calculate tasks per thread, rounding up to handle remainders
  int tasks_per_thread = (num_total_tasks + num_threads - 1) / num_threads;

  // Launch threads
  for (int i = 0; i < num_threads; i++) {
    int start_task = i * tasks_per_thread;
    int end_task = std::min(start_task + tasks_per_thread, num_total_tasks);
    // Only spawn thread if there are tasks for it
    if (start_task < num_total_tasks) {
      auto taskWrapper = [runnable, start_task, end_task, num_total_tasks]() {
        for (int j = start_task; j < end_task; j++) {
          runnable->runTask(j, num_total_tasks);
        }
      };

      threads.push_back(std::thread(taskWrapper));
    }
  }

  // Wait for all threads to complete
  for (auto &thread : threads) {
    thread.join();
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads) {
  // Initialize member variables
  this->num_threads = num_threads;
  this->should_exit = false;
  this->has_work = false;
  this->tasks_completed = 0;

  // Create the thread pool
  for (int i = 0; i < num_threads; i++) {
    threads.push_back(std::thread([this]() { this->workerThreadLoop(); }));
  }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  // Signal threads to exit and wait for them
  {
    std::lock_guard<std::mutex> lock(mutex);
    should_exit = true;
    has_work = true;
  }
  work_cv.notify_all();

  for (auto &thread : threads) {
    thread.join();
  }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable,
                                               int num_total_tasks) {
  {
    std::lock_guard<std::mutex> lock(mutex);
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    this->next_task = 0;
    this->tasks_completed = 0;
    this->has_work = true;
  }

  // Wake up worker threads
  work_cv.notify_all();

  // Wait for all tasks to complete
  while (true) {
    std::lock_guard<std::mutex> lock(mutex);
    if (tasks_completed == num_total_tasks) {
      break;
    }
  }

  // Reset state
  {
    std::lock_guard<std::mutex> lock(mutex);
    has_work = false;
    runnable = nullptr;
  }
}

void TaskSystemParallelThreadPoolSpinning::workerThreadLoop() {
  while (true) {
    // Check if thread should exit
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (should_exit) {
        return;
      }
    }

    // Try to get a task
    int task_id = -1;
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (has_work && next_task < num_total_tasks) {
        task_id = next_task++;
      }
    }

    // If we got a valid task, execute it
    if (task_id >= 0) {
      runnable->runTask(task_id, num_total_tasks);

      // Update completion count
      {
        std::lock_guard<std::mutex> lock(mutex);
        tasks_completed++;
        if (tasks_completed == num_total_tasks) {
          has_work = false;
          work_cv.notify_all();
        }
      }
    }
    // If no task available, keep spinning
  }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // You do not need to implement this method.
  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // You do not need to implement this method.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads) {
  // Initialize member variables
  this->num_threads = num_threads;
  this->should_exit = false;
  this->has_work = false;
  this->tasks_completed = 0;

  // Create the thread pool
  for (int i = 0; i < num_threads; i++) {
    threads.push_back(std::thread([this]() { this->workerThreadLoop(); }));
  }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  // Signal threads to exit and wait for them
  {
    std::lock_guard<std::mutex> lock(mutex);
    should_exit = true;
    has_work = true;
  }
  work_cv.notify_all();

  for (auto &thread : threads) {
    thread.join();
  }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {
  {
    std::lock_guard<std::mutex> lock(mutex);
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    this->next_task = 0;
    this->tasks_completed = 0;
    this->has_work = true;
  }

  // Wake up worker threads
  work_cv.notify_all();

  // Wait for all tasks to complete
  std::unique_lock<std::mutex> lock(mutex);
  completion_cv.wait(lock, [this, num_total_tasks]() {
    return tasks_completed == num_total_tasks;
  });

  // Reset state
  has_work = false;
  runnable = nullptr;
}

void TaskSystemParallelThreadPoolSleeping::workerThreadLoop() {
  while (true) {
    std::unique_lock<std::mutex> lock(mutex);

    // Wait for work or exit signal
    work_cv.wait(lock, [this]() { return has_work || should_exit; });

    // Check if thread should exit
    if (should_exit) {
      return;
    }

    // Try to get a task
    while (has_work && next_task < num_total_tasks) {
      int task_id = next_task++;
      lock.unlock();

      // Execute the task
      runnable->runTask(task_id, num_total_tasks);

      lock.lock();
      tasks_completed++;
      if (tasks_completed == num_total_tasks) {
        has_work = false;
        completion_cv.notify_one();
      }
    }
  }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {

  //
  // TODO: CS149 students will implement this method in Part B.
  //

  return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //

  return;
}

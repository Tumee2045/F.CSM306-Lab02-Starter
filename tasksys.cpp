#include "tasksys.h"
#include <algorithm>
#include <thread>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads)
{
    num_threads_ = std::max(1, num_threads);
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
    {
        return;
    }

    std::vector<std::thread> threads;
    threads.reserve(num_threads_);

    for (int t = 0; t < num_threads_; t++)
    {
        threads.emplace_back([=]()
                             {
            int start = (t * num_total_tasks) / num_threads_;
            int end = ((t + 1) * num_total_tasks) / num_threads_;
            for (int i = start; i < end; i++)
            {
                runnable->runTask(i, num_total_tasks);
            } });
    }

    for (size_t i = 0; i < threads.size(); i++)
    {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    num_threads_ = std::max(1, num_threads);
    current_runnable_ = nullptr;
    total_tasks_ = 0;
    next_task_ = 0;
    completed_tasks_ = 0;
    has_work_ = false;
    shutting_down_ = false;

    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++)
    {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    {
        std::lock_guard<std::mutex> lock(mtx_);
        shutting_down_ = true;
    }

    for (size_t i = 0; i < workers_.size(); i++)
    {
        workers_[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop()
{
    while (true)
    {
        int task_id = -1;
        int total = 0;
        IRunnable *runnable = nullptr;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (shutting_down_)
            {
                return;
            }

            if (has_work_ && next_task_ < total_tasks_)
            {
                task_id = next_task_++;
                total = total_tasks_;
                runnable = current_runnable_;
            }
        }

        if (task_id < 0)
        {
            std::this_thread::yield();
            continue;
        }

        runnable->runTask(task_id, total);

        {
            std::lock_guard<std::mutex> lock(mtx_);
            completed_tasks_++;
            if (completed_tasks_ == total_tasks_)
            {
                has_work_ = false;
            }
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
    {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mtx_);
        current_runnable_ = runnable;
        total_tasks_ = num_total_tasks;
        next_task_ = 0;
        completed_tasks_ = 0;
        has_work_ = true;
    }

    while (true)
    {
        bool done = false;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            done = (completed_tasks_ == total_tasks_) && !has_work_;
        }

        if (done)
        {
            break;
        }

        std::this_thread::yield();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads)
{
    num_threads_ = std::max(1, num_threads);
    current_runnable_ = nullptr;
    total_tasks_ = 0;
    next_task_ = 0;
    completed_tasks_ = 0;
    has_work_ = false;
    shutting_down_ = false;

    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++)
    {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    {
        std::lock_guard<std::mutex> lock(mtx_);
        shutting_down_ = true;
    }
    cv_work_.notify_all();

    for (size_t i = 0; i < workers_.size(); i++)
    {
        workers_[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::workerLoop()
{
    while (true)
    {
        int task_id = -1;
        int total = 0;
        IRunnable *runnable = nullptr;

        {
            std::unique_lock<std::mutex> lock(mtx_);
            cv_work_.wait(lock, [&]()
                          { return shutting_down_ || (has_work_ && next_task_ < total_tasks_); });

            if (shutting_down_)
            {
                return;
            }

            task_id = next_task_++;
            total = total_tasks_;
            runnable = current_runnable_;
        }

        runnable->runTask(task_id, total);

        {
            std::lock_guard<std::mutex> lock(mtx_);
            completed_tasks_++;
            if (completed_tasks_ == total_tasks_)
            {
                has_work_ = false;
                cv_done_.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
    {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mtx_);
        current_runnable_ = runnable;
        total_tasks_ = num_total_tasks;
        next_task_ = 0;
        completed_tasks_ = 0;
        has_work_ = true;
    }

    cv_work_.notify_all();

    std::unique_lock<std::mutex> lock(mtx_);
    cv_done_.wait(lock, [&]()
                  { return (completed_tasks_ == total_tasks_) && !has_work_; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    //
    // TODO: CSM306 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CSM306 students will modify the implementation of this method in Part B.
    //

    return;
}
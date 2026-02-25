#include <iostream>
#include <vector>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <thread>
#include "tasksys.h"

// Тооцоолол хийх "Ажил" (Task) класс
class ComputeTask : public IRunnable
{
public:
    std::vector<double> results;
    int workload_intensity;

    ComputeTask(int num_tasks, int intensity)
        : results(num_tasks, 0.0), workload_intensity(intensity) {}

    void runTask(int taskID, int num_total_tasks) override
    {
        double val = 0.0;
        for (int i = 0; i < workload_intensity; ++i)
        {
            val += std::sin(i * 0.01 + taskID) * std::cos(i * 0.02 + taskID);
        }
        results[taskID] = val;
    }
};

void runBenchmark(ITaskSystem *system, IRunnable *task, int num_tasks, const std::string &name)
{
    std::cout << "Testing [" << name << "]..." << std::flush;
    auto start = std::chrono::high_resolution_clock::now();
    system->run(task, num_tasks);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << " Done. Time: " << std::fixed << std::setprecision(4) << elapsed.count() << "s" << std::endl;
}

int main()
{
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0)
        num_threads = 4;

    // Ажлын тоо их, нэг ажлын хүндрэл бага байх тусам
    // Thread Pool-ийн давуу тал тодорхой харагдана.
    int num_tasks = 5000;
    int workload_intensity = 200;

    std::cout << "========================================" << std::endl;
    std::cout << "Task System Benchmark" << std::endl;
    std::cout << "Threads: " << num_threads << ", Tasks: " << num_tasks << std::endl;
    std::cout << "========================================" << std::endl;

    ComputeTask task(num_tasks, workload_intensity);

    // 1. Serial System
    ITaskSystem *serialSystem = new TaskSystemSerial(num_threads);
    runBenchmark(serialSystem, &task, num_tasks, "Serial System");
    delete serialSystem;

    // 2. Parallel Spawn System (Step 1)
    ITaskSystem *spawnSystem = new TaskSystemParallelSpawn(num_threads);
    runBenchmark(spawnSystem, &task, num_tasks, "Parallel Spawn");
    delete spawnSystem;

    // 3. Parallel Spinning System (Step 2)
    ITaskSystem *spinningSystem = new TaskSystemParallelThreadPoolSpinning(num_threads);
    runBenchmark(spinningSystem, &task, num_tasks, "Parallel Spinning Pool");
    delete spinningSystem;

    // 4. Parallel Sleeping System (Step 3)
    ITaskSystem *sleepingSystem = new TaskSystemParallelThreadPoolSleeping(num_threads);
    runBenchmark(sleepingSystem, &task, num_tasks, "Parallel Sleeping Pool");
    delete sleepingSystem;

    return 0;
}
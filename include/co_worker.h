// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef BAIDU_COMMON_CO_WORKER_H_
#define BAIDU_COMMON_CO_WORKER_H_

#include <stdint.h>
#include <queue>
#include <map>
#include <functional>
#include <atomic>

#include <boost/context/all.hpp>

#include <mutex.h>
#include <thread.h>

namespace baidu {
namespace common {

#define TASK_STACK_SIZE 256 * 1024

enum CoTaskStat {
    RUNNABLE,
    YIELD,
    ENDED
};

typedef void (*UserFunc)(void*);

struct CoTask {
    CoTask() {}
    CoTask(UserFunc user_fn, void* user_arg) :
        fn(user_fn), arg(user_arg), stack(NULL),
        stack_size(0), id(-1), stat(RUNNABLE) {}
    ~CoTask() {
        delete[] stack;
    }
    void (*fn)(void*);
    void* arg;
    char* stack;
    int32_t stack_size;
    int64_t id;
    boost::context::fcontext_t context;
    CoTaskStat stat;
};

class CoWorker {
public:
    CoWorker();
    int64_t AddCoTask(void (*fn)(void*), void* arg);
    void RemoveCoTask(int64_t task_id);
    static void YielCoTask();
    void ResumeCoTask(int64_t task_id);
private:
    static void TaskWrapper(intptr_t para);
    static void PrepareStackForTask(CoTask* task);
    static void CleanCoTask(CoTask* task);
    void SwitchToCoTask(CoTask* task);
    void RunMainTask();
    void YielCurrentCoTask();
    CoTask* SelectNextTask();
    typedef std::queue<CoTask*> RunnableTaskQueue;
    typedef std::map<int64_t, CoTask*> YieledTaskQueue;

    YieledTaskQueue yieled_queue_;
    RunnableTaskQueue runnable_queue_;
    //TODO encode thread_id into next_task_id_
    int64_t next_task_id_;
    Mutex mu_;
    CondVar cond_;
    CoTask* cur_task_;
    CoTask* ended_task_;
    CoTask main_task_;
    Thread work_thread_;
};

} // namespace common
} // namespace baidu

#endif // BAIDU_COMMON_CO_WORKER_H_

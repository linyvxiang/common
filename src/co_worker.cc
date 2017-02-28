// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "co_worker.h"
#include "logging.h"

#include <assert.h>

namespace baidu {
namespace common {

static CoWorker* co_worker = NULL;

CoWorker* GetCoWorker() {
    return co_worker;
}

static inline void jump_context(boost::context::fcontext_t* cur_pos,
                         boost::context::fcontext_t target_pos,
                         intptr_t para)
{
    boost::context::jump_fcontext(cur_pos, target_pos, para);
}

CoWorker::CoWorker() : next_task_id_(1), cond_(&mu_),
                       cur_task_(NULL), ended_task_(NULL)

{
    //TODO need to be thread-safe here?
    co_worker = this;
    work_thread_.Start(std::bind(&CoWorker::RunMainTask, this));
}

int64_t CoWorker::AddCoTask(UserFunc fn, void* arg)
{
    CoTask* task = new CoTask(fn, arg);
    MutexLock lock(&mu_);
    runnable_queue_.push(task);
    task->id = next_task_id_++;
    cond_.Signal();
    return task->id;
}

void CoWorker::RemoveCoTask(int64_t task_id)
{

}

CoTask* CoWorker::SelectNextTask()
{
    // try run next task, if none, jump to main task
    CoTask* next_task = NULL;
    MutexLock lock(&mu_);
    if (runnable_queue_.empty()) {
        LOG(DEBUG, "Switch to main task");
        next_task = &(main_task_);
    } else {
        next_task = (runnable_queue_.front());
        LOG(DEBUG, "Pop task %ld from queue", next_task->id);
        runnable_queue_.pop();
    }
    return next_task;
}

void CoWorker::YielCoTask()
{
    CoWorker* worker = GetCoWorker();
    worker->YielCurrentCoTask();
}

void CoWorker::ResumeCoTask(int64_t task_id)
{
    CoTask* task = NULL;
    {
        MutexLock lock(&mu_);
        auto it = yieled_queue_.find(task_id);
        assert(it != yieled_queue_.end());
        task = it->second;
        assert(task->stat = YIELD);
        task->stat = RUNNABLE;
        //TODO insert to the front of queue?
        runnable_queue_.push(task);
        LOG(DEBUG, "Resume task %ld", task->id);
        cond_.Signal();
    }
}

void CoWorker::YielCurrentCoTask()
{
    LOG(DEBUG, "Yiel task %ld", cur_task_->id);
    assert(cur_task_->stat == RUNNABLE);
    cur_task_->stat = YIELD;
    {
        MutexLock lock(&mu_);
        yieled_queue_[cur_task_->id] = cur_task_;
    }
    CoTask* next_task = SelectNextTask();
    SwitchToCoTask(next_task);
}

void CoWorker::RunMainTask()
{
    main_task_.stack = new char[TASK_STACK_SIZE];
    main_task_.stack_size = TASK_STACK_SIZE;
    main_task_.fn = NULL;
    main_task_.arg = NULL;
    main_task_.id = 0;
    main_task_.stat = RUNNABLE;
    //TODO necessary here?
    main_task_.context = boost::context::make_fcontext(main_task_.stack +
                                                       TASK_STACK_SIZE,
                                                       main_task_.stack_size,
                                                       NULL);
    cur_task_ = &main_task_;

    while (1) {
        CoTask* task = NULL;
        {
            MutexLock lock(&mu_);
            while (runnable_queue_.empty()) {
                LOG(DEBUG, "task queue empty, wait for task");
                cond_.Wait();
            }
            task = runnable_queue_.front();
            runnable_queue_.pop();
        }
        SwitchToCoTask(task);
        if (ended_task_ && ended_task_->stat == ENDED) {
            CleanCoTask(ended_task_);
            ended_task_ = NULL;
        }
    }
}

void CoWorker::TaskWrapper(intptr_t /*para*/)
{
    CoWorker* worker = GetCoWorker();
    if (worker->ended_task_ && worker->ended_task_->stat == ENDED) {
        // free task here
        CleanCoTask(worker->ended_task_);
    }

    worker->cur_task_->fn(worker->cur_task_->arg);

    worker->cur_task_->stat = ENDED;
    CoTask* next_task = worker->SelectNextTask();
    worker->SwitchToCoTask(next_task);
}

void CoWorker::SwitchToCoTask(CoTask* task)
{
    LOG(DEBUG, "Switch to task %ld", task->id);
    if (!task->stack) {
        PrepareStackForTask(task);
    }
    ended_task_ = cur_task_;
    cur_task_ = task;
    // jump to task stack
    jump_context(&(ended_task_->context), cur_task_->context, 0);
}

void CoWorker::PrepareStackForTask(CoTask* task)
{
    assert(task->stack == NULL);
    task->stack = new char[TASK_STACK_SIZE];
    task->stack_size = TASK_STACK_SIZE;
    task->context = boost::context::make_fcontext(task->stack + TASK_STACK_SIZE,
                                                  TASK_STACK_SIZE,
                                                  &CoWorker::TaskWrapper);
}

void CoWorker::CleanCoTask(CoTask* task)
{
    LOG(DEBUG, "Clean task %ld", task->id);
    delete task;
}

} // namespace common
} // namespace baidu

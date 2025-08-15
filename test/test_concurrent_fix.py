#!/usr/bin/env python3
"""
测试修复后的UniversalLLM并发性能
"""

import time
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import sys
import os

from llm_api.universal_llm import create_llm, UniversalLLM


def test_single_thread():
    """单线程测试"""
    print("=== 单线程测试 ===")
    llm = create_llm("base")  # 使用base配置，有多个provider
    
    try:
        start_time = time.time()
        response = llm.generate("简单回答：1+1等于几？", verbose=True)
        end_time = time.time()
        
        print(f"✅ 单线程测试成功")
        print(f"回复: {response}")
        print(f"耗时: {end_time - start_time:.2f}秒")
        print(f"使用provider: {llm.current_provider}")
        return True
    except Exception as e:
        print(f"❌ 单线程测试失败: {e}")
        return False


def thread_worker(thread_id, prompt):
    """线程工作函数"""
    try:
        llm = create_llm("base")
        start_time = time.time()
        response = llm.generate(f"{prompt} (线程{thread_id})", verbose=False)
        end_time = time.time()
        
        return {
            'thread_id': thread_id,
            'success': True,
            'response': response,
            'provider': llm.current_provider,
            'duration': end_time - start_time
        }
    except Exception as e:
        return {
            'thread_id': thread_id,
            'success': False,
            'error': str(e),
            'duration': time.time() - start_time
        }


def test_concurrent_threads():
    """多线程并发测试"""
    print("\n=== 多线程并发测试 ===")
    
    # 重置实例确保干净的状态
    UniversalLLM.reset_instance()
    
    num_threads = 5
    prompts = [f"简单回答数字：{i}" for i in range(1, num_threads + 1)]
    
    results = []
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # 提交所有任务
        future_to_thread = {}
        for i, prompt in enumerate(prompts):
            future = executor.submit(thread_worker, i+1, prompt)
            future_to_thread[future] = i+1
        
        # 收集结果
        for future in as_completed(future_to_thread):
            result = future.result()
            results.append(result)
            thread_id = result['thread_id']
            
            if result['success']:
                print(f"✅ 线程{thread_id}完成 ({result['duration']:.2f}s)")
            else:
                print(f"❌ 线程{thread_id}失败: {result['error']}")
    
    total_time = time.time() - start_time
    successful = len([r for r in results if r['success']])
    
    print(f"\n多线程测试结果:")
    print(f"总耗时: {total_time:.2f}秒")
    print(f"成功: {successful}/{num_threads}")
    
    # 显示状态
    llm = create_llm("base")
    status = llm.get_status()
    print(f"超时配置: {status['timeout_config']}")
    print(f"可用providers: {status['available_providers']}")
    print(f"已初始化clients: {status['initialized_clients']}")
    
    return successful == num_threads


def process_worker(process_id):
    """进程工作函数"""
    try:
        # 每个进程创建自己的LLM实例
        llm = create_llm("base")
        start_time = time.time()
        response = llm.generate(f"进程{process_id}问题: 2x{process_id}等于几？", verbose=False)
        end_time = time.time()
        
        return {
            'process_id': process_id,
            'success': True,
            'response': response,
            'provider': llm.current_provider,
            'duration': end_time - start_time,
            'pid': os.getpid()
        }
    except Exception as e:
        return {
            'process_id': process_id,
            'success': False,
            'error': str(e),
            'duration': 0,
            'pid': os.getpid()
        }


def test_concurrent_processes():
    """多进程并发测试"""
    print("\n=== 多进程并发测试 ===")
    
    # 重置实例确保干净的状态
    UniversalLLM.reset_instance()
    
    num_processes = 3
    results = []
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        # 提交所有任务
        future_to_process = {}
        for i in range(1, num_processes + 1):
            future = executor.submit(process_worker, i)
            future_to_process[future] = i
        
        # 收集结果
        for future in as_completed(future_to_process):
            result = future.result()
            results.append(result)
            process_id = result['process_id']
            
            if result['success']:
                print(f"✅ 进程{process_id}完成 (PID:{result['pid']}, {result['duration']:.2f}s)")
            else:
                print(f"❌ 进程{process_id}失败: {result['error']}")
    
    total_time = time.time() - start_time
    successful = len([r for r in results if r['success']])
    
    print(f"\n多进程测试结果:")
    print(f"总耗时: {total_time:.2f}秒")
    print(f"成功: {successful}/{num_processes}")
    
    return successful == num_processes


def main():
    """主测试函数"""
    print("开始测试修复后的UniversalLLM并发性能...\n")
    
    # 单线程测试
    single_success = test_single_thread()
    
    # 多线程测试
    thread_success = test_concurrent_threads()
    
    # 多进程测试
    process_success = test_concurrent_processes()
    
    # 总结
    print(f"\n=== 测试总结 ===")
    print(f"单线程测试: {'✅ 通过' if single_success else '❌ 失败'}")
    print(f"多线程测试: {'✅ 通过' if thread_success else '❌ 失败'}")
    print(f"多进程测试: {'✅ 通过' if process_success else '❌ 失败'}")
    
    # 清理资源
    print(f"\n清理资源...")
    UniversalLLM.reset_instance()
    
    return single_success and thread_success and process_success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
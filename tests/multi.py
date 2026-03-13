import multiprocessing as mp
import time
import dill

def example_function2(x, y=10, q=None):
    print(f"Running with x={x}, y={y}")
    time.sleep(1)
    result = x+y
    if q is not None:
        q.put(result)
    return result

def _worker(serialized_func, serialized_args, serialized_kwargs, result_queue=None):
    """
    Global wrapper: deserialize function & arguments, call function,
    and optionally put the result into a queue.
    """
    func = dill.loads(serialized_func)
    args = dill.loads(serialized_args) if serialized_args is not None else ()
    kwargs = dill.loads(serialized_kwargs) if serialized_kwargs is not None else {}
    result = func(*args, **kwargs)
    if result_queue is not None:
        result_queue.put(result)

def create_process(func, args=None, kwargs=None, result_queue=None):
    """
    Create a Process that runs func(*args, **kwargs).
    If result_queue is given, the return value is put into it.
    """
    serialized_func = dill.dumps(func)
    serialized_args = dill.dumps(args) if args is not None else None
    serialized_kwargs = dill.dumps(kwargs) if kwargs is not None else None
    p = mp.Process(target=_worker, args=(serialized_func, serialized_args, serialized_kwargs, result_queue))
    return p
"""
The issue is that your script lacks the if __name__ == '__main__': guard. 
When you use the 'spawn' start method (common on Windows, and forced here), 
each child process re-imports the main module. Without the guard, the child 
will execute the same top‑level code: it creates another Queue, sets the 
start method again, and starts another process – which does the same, 
leading to an infinite chain of processes. Each process then waits for its 
own child to finish (because of p.join()), so none of them ever reach 
the actual target function example_function2. Consequently, nothing is ever 
put into the original queue, and q.get(0) raises queue.Empty (or blocks forever 
if you use a blocking call).
"""
if __name__ == '__main__':

    q = mp.Queue()

    mp.set_start_method('spawn', force=True)
    p = mp.Process(target=example_function2,args=(5, 20, q))

    p.start()
    p.join()
    print(q.get(0))

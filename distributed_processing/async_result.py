from time import time
from datetime import datetime
import logging
from .exceptions import RemoteException

logger = logging.getLogger(__name__)

def timestamp():
    return datetime.now().isoformat()

PENDING = 'PENDING'
OK = 'OK'
FAILED = 'FAILED'
CLEANED = 'CLEANED'

class AsyncResult(object):

    def __init__(self, rpc_client, id, request=None, queue=None):
        self._client = rpc_client
        self.id = id
        self._status = PENDING
        self.value = None
        self.error = None
        self.creation_time = time()
        self.finished_time = None
        self.request = request
        self.queue = queue
        self.retries = 0
        self.metadata = {}

    def ok(self):
        return self.status == OK

    def failed(self):
        return self.status == FAILED
    
    def done(self):
        return self.ok() or self.failed()

    def pending(self):
        """Returns True if the state of the AsyncResult object is 'PENDING'.

        Syncs the object with rpc_client, just in case we have used wait_responses 
        from the client or if there are responses available in the client queue.

        'PENDING' state should be assumed as transitory.

        Returns:
            bool: True if 'PENDING', False otherwise   
        """
        return self.status == PENDING
    
    def _raise_exception(self, error):
        """Raises a RemoteException with the information in error.

        Args:
            error (dict): Dictionary with "code", "message" and/or "trace" as keys 
                and a str as value.

        Raises:
            RemoteException  
        """
        raise RemoteException(error)
    
    @property
    def status(self):
        """Returns the status of the AsyncResult object.

        Syncs the object with rpc_client, just in case we have used wait_responses 
        from the client or if there are responses available in the client queue.

        'PENDING' state should be assumed as transitory. 

        Returns:
            str: 'PENDING', 'OK' or 'FAILED'   
        """
        if self._status == PENDING:
            try:
                self.wait(timeout=0) 
            except TimeoutError:
                pass
        return self._status

    def get(self, timeout=None, clean=True):
        """Returns the value of the AsyncResult object.
        
        Throws a RemoteException exception with the information 
        in "error" of the response message.
        
        Args:
            timeout (float, optional): Defaults to None (rpc_client.timeout).
                If 0, check queue once.
            clean (bool, optional): If True remove the result from cache.
                Defaults to True. 
        
        Returns:
            result

        Raises:
            TimeoutError
            RemoteException
        """      
        self.wait(timeout, clean)
        if self.ok():
            return self.value
        elif self.failed():
            self._raise_exception(self.error)
        raise ValueError("AsyncResult: Undefined Value.") # shouldn`t happen

    def wait(self, timeout=None, clean=True):
        """Waits for result and updates the AsyncResult object.
       
        Throws TimeoutError if timeout reached.

        Args:
            timeout (float, optional): Defaults to None (rpc_client.timeout).
                If 0, check queue once.
            clean (bool, optional): If True remove the result from cache.
                Defaults to True. 
      
        Raises:
            TimeoutError
        """
        if self._status == PENDING:
            response = self._client.wait_one_response(self.id, timeout, clean=clean)
            if "result" in response:
                self.finished_time = response["finished_time"] # Included by Client, not in message
                self._status = OK
                self.value = response["result"]
                self.metadata = response.get("metadata", {})

            elif "error" in response:
                self.finished_time = response["finished_time"] # Included by Client, not in message
                self._status = FAILED
                self.error = response["error"]
                self.metadata = response.get("metadata", {})


    def safe_get(self, timeout=None, clean=True, default=None):
        try:
            return self.get(timeout, clean=clean)
        except:
            return default
        
    def retry(self, queue=None):
        """Retries the request linked to the AsyncResult instance.

        Only retries if the request is pending.
        
        Args:
            queue (str, optional): queue to sent the request. Defaults to None.
                If None, selects the queue based on:
                - Available queues for the method if client's `check_registry` is 'always' or 'cache'
                - Client's `default_requests_queue` attribute otherwise.

        Returns:
            bool: True if the request has been retried, False if not (request already received).
        
        """
        if self.request is None:
            raise ValueError("AsyncResult.retry(): request info is None. Can not retry.")
        if self.pending():
            method, args, kwargs = self.request
            new_id, new_queue = self._client.send_single_request(method, args, kwargs, self.id, queue=queue)
            logger.debug(f"{timestamp()} Client: {self._client.client_id} Retrying request with id: {self.id} to queue: {new_queue}")
            self.queue = queue
            assert new_id == self.id
            self.retries += 1
        else:
            logger.debug(f"{timestamp()} Client: {self._client.client_id} Not Retrying: Response to Request with id: {self.id} already received.")

def gather(fs, timeout=None, step=5, max_dt=30):
    """Gathers all AsyncResults in the list.
    
    Args:
        timeout (int, float, optional): Timeout in seconds. Maximum waiting time. 
            Defaults to None. If None, unlimited waiting time.
        step (int, float): Step time in seconds. Defaults to 5 secs.
        max_dt (int, float): Max delta time to retry a request.

    Returns:
        bool: True if the request has been retried, False if not (request already received).
    """


    def ar_indices_to_retry(fs):
        done = [(f.queue, (i, f.ok() or f.failed())) for i, f in enumerate(fs)]

        d = {}
        for x in done:
            d.setdefault(x[0], []).append(x[1])

        salida = []
        for k, l in d.items():
            for i in range(len(l)-1, 0, -1):
                if l[i][1]:
                    break
            for i in range(0, i):
                if not l[i]:
                    salida.append((k,i))

        return [x[1] for x in salida]
    
    _ = [f.status for f in fs]
    # AsyncResults sorted by creation_time
    fs = [x[0] for x in sorted([(f , f.creation_time) for f in fs], key=lambda x:x[1])]

    N = len(fs)
    t_0 = time()
    last_time = t_0
    i = 0
    N_pending = N
    max_dt_real = max_dt
    s = 0
    retried = set()
    while (time() - t_0) <= timeout:
        pending_ars = [ar for ar in fs if not(ar.ok() or ar.failed())]
        pending_ids = [ar.id for ar in pending_ars]
        
        if len(pending_ars) < N_pending:
            N_pending = len(pending_ars)
            last_time = time()
            s = 0
        
        dts = [f.metadata["timing"]["execution_finish"]- f.metadata["timing"]["execution_start"] 
                for f in fs 
                if f.metadata != {} and (f.ok() or f.failed())]

        if len(dts)>0:
            max_dt_real = max(max(dts), max_dt)

        if (time() - last_time) > 2 * max_dt_real:
            for ix in ar_indices_to_retry(fs):
                if ix not in retried:
                    fs[ix].retry()
                    retried.add(ix)
                logger.debug(f"Retrying request with id: {fs[ix].id}")    
            logger.warning(f"{s} It looks like there are not workers on the other side.") 
        
        logger.debug(f"{i}: seconds {time()-t_0}s, AR recovered {N-N_pending}, AR left {N_pending}, max delta {max_dt_real}")
        
        try:
            if fs[0]._client.wait_responses(pending_ids, timeout=step) == []: #asume all request were sent bay the same client
                return
        except TimeoutError:
            pass
      
        _ = [f.status for f in fs]

        [(f.ok() or f.failed(), f.queue) for f in fs]

        i += 1

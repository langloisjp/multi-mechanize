#
#  Copyright (c) 2013 Jean-Philippe Langlois (jpl@jplanglois.com)
#  License: GNU LGPLv3
#
"""
Proxy test script schedules work according to weights and rates.

Multi-Mechanize calls test scripts as fast as it can. You can
control how many threads are used for each test script. That's
a little limited. With this test script, you can define
weights as well as a target call rate.

Input:
- rate of transactions per second: e.g. 10. A number <= 0 means go
as fast as you can.
- transaction classes and weights. Each transaction class will be
called according to given weights. Weights are relative; absolute
values don't matter. For example, you could have a 'get' transaction
weighed at 10 and a 'put' transaction weighed at 1. This means that
the 'get' transaction will be called 10 times more than the 'put'
transaction.

"""

import random
import time
import wlsclient
import testconf
import operator
import sys

# Define rate and work here.
RATE = 10
WORK = {
    'script1': 90,
    'script2': 9,
    'script3': 1
}

# Multi-mech uses the Transaction call
class Transaction(object):
    def __init__(self):
        self.scheduler = Scheduler(RATE, WORK)

    def run(self):
        self.scheduler.run()
        self.custom_timers = self.scheduler.custom_timers


class Scheduler(object):
    def __init__(self, rate, work):
        """Initialize the rate worker object. Will schedule
        transactions according to work specifications.

        Work specification is a dictionary of module -> weight, e.g.:

            w = Scheduler(10, {'module1': 9, 'module2': 1})

        """
        self.rate = rate
        self.duration = 1.0 / rate
        self.work = work
        self.counts = dict([(worker, 0) for worker in work.keys()])

        self.workers = {}
        for worker_name in self.work.keys():
            m = __import__(worker_name)
            worker = m.Transaction()
            self.workers[worker_name] = worker

        self.custom_timers = {}

    def run(self):
        """Run one iteration.
        Schedules the next call based on work specifications.
        Sleeps as necessary to limit rate (+/- 10% jitter).
        """
        start_time = time.time()

        worker_name = self.next_worker()
        worker = self.workers[worker_name]

        try:
            worker.custom_timers = {}
            worker.run()
            self.custom_timers = worker.custom_timers
        except Exception, e:
            log('Caught exception: {}'.format(str(e)))

        self.counts[worker_name] += 1

        wait_time = self.duration - (time.time() - start_time)
        wait_time *= 1.0 + random.uniform(-0.1, 0.1) # up to 10% jitter
        if wait_time > 0: time.sleep(wait_time)

    def next_worker(self):
        """Return which worker should be called next
        We're keeping track of counts, so we can calculate
        the current ratio of actual calls so far
        compared with the target, and pick the worker furthest
        from its goal.
        """
        ratios = call_ratios(self.work, self.counts)
        (next_worker_name, _) = min_ratio(ratios)
        return next_worker_name


def call_ratios(work, counts):
    """Calculate how far each worker is from its target ratio.

    work: dictionary {worker: weight}
    counts: dictionary {worker: count}

    Returns a list of (worker, distance) tuples.

    >>> call_ratios({'a': 10, 'b': 1}, {'a': 0, 'b': 0})
    [('a', 0.0), ('b', 0.0)]
    >>> call_ratios({'a': 10, 'b': 1}, {'a': 1, 'b': 0})
    [('a', 0.1), ('b', 0.0)]
    """
    return [(worker, float(counts[worker]) / float(work[worker])) for worker in work.keys()]

def min_ratio(ratios):
    """Returns the minimum worker ratio.
    ratios is a list of (worker, ratio) tuples.

    >>> min_ratio([('a', 0.1), ('b', 0.0)])
    ('b', 0.0)
    """
    (min_worker, min_ratio) = ratios[0]
    for (worker, ratio) in ratios[1:]:
        if ratio < min_ratio:
            min_worker = worker
            min_ratio = ratio

    return (min_worker, min_ratio)

def log(msg):
    sys.stderr.write(msg)
    sys.stderr.write('\n')


# You can run the test script manually outside
# of multi-mech. Of course no data will be captured
# or reported.
# Runs 1 iteration unless you specify more on the
# command line.
if __name__ == '__main__':
    import sys
    try:
        count = int(sys.argv[1])
    except:
        count = 1
    trans = Transaction()
    for i in xrange(count):
        trans.run()
        print trans.custom_timers


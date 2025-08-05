from skopt.space import Integer
from skopt import Optimizer
from scipy.optimize import minimize
from collections import OrderedDict
import numpy as np
import time

exit_signal = 10 ** 10
cc_change_limit = 5

def base_optimizer(configurations, black_box_function, logger, verbose=True):
    limit_obs, count = 20, 0
    max_thread = configurations["thread_limit"]
    iterations = configurations["bayes"]["num_of_exp"]
    mp_opt = configurations["mp_opt"]

    if mp_opt:
        search_space  = [
            Integer(1, max_thread), # Concurrency
            Integer(1, max_thread), # Parallesism
            # Integer(1, 10), # Pipeline
            # Integer(5, 20), # Chunk/Block Size in KB: power of 2
        ]
    else:
        search_space  = [
            Integer(1, max_thread), # Concurrency
        ]

    params = []
    optimizer = Optimizer(
        dimensions=search_space,
        base_estimator="GP", #[GP, RF, ET, GBRT],
        acq_func="gp_hedge", # [LCB, EI, PI, gp_hedge]
        acq_optimizer="auto", #[sampling, lbfgs, auto]
        n_random_starts=configurations["bayes"]["initial_run"],
        model_queue_size= limit_obs,
        # acq_func_kwargs= {},
        # acq_optimizer_kwargs={}
    )

    while True:
        count += 1

        if len(optimizer.yi) > limit_obs:
            optimizer.yi = optimizer.yi[-limit_obs:]
            optimizer.Xi = optimizer.Xi[-limit_obs:]

        if verbose:
            logger.info("Iteration {0} Starts ...".format(count))

        t1 = time.time()
        res = optimizer.run(func=black_box_function, n_iter=1)
        t2 = time.time()

        if verbose:
            logger.info("Iteration {0} Ends, Took {3} Seconds. Best Params: {1} and Score: {2}.".format(
                count, res.x, res.fun, np.round(t2-t1, 2)))

        last_value = optimizer.yi[-1]
        if last_value == exit_signal:
            logger.info("Optimizer Exits ...")
            break

        cc = optimizer.Xi[-1][0]
        if iterations < 1:
            reset = False
            if (last_value > 0) and (cc < max_thread):
                max_thread = max(cc, 2)
                reset = True

            if (last_value < 0) and (cc == max_thread) and (cc < configurations["network_thread_limit"]):
                max_thread = min(cc+5, configurations["network_thread_limit"])
                reset = True

            if reset:
                search_space[0] = Integer(1, max_thread)
                optimizer = Optimizer(
                    dimensions=search_space,
                    n_initial_points=configurations["bayes"]["initial_run"],
                    acq_optimizer="lbfgs",
                    model_queue_size= limit_obs
                )

        if iterations == count:
            logger.info("Best parameters: {0} and score: {1}".format(res.x, res.fun))
            params = res.x
            break

    return params

def run_probe(current_cc, count, verbose, logger, black_box_function):
    if verbose:
        logger.info("Iteration {0} Starts ...".format(count))

    t1 = time.time()
    current_value = black_box_function(current_cc)
    t2 = time.time()

    if verbose:
        logger.info("Iteration {0} Ends, Took {1} Seconds. Score: {2}.".format(
            count, np.round(t2-t1, 2), current_value))

    return current_value

def gradient_opt_fast(max_cc, black_box_function, logger, verbose=True):
    count = 0
    cache = OrderedDict()
    values = []
    ccs = [1]

    while True:
        count += 1
        soft_limit = max_cc
        values.append(run_probe([ccs[-1]], count, verbose, logger, black_box_function))
        cache[abs(values[-1])] = ccs[-1]

        if count % 10 == 0:
            soft_limit = min(cache[max(cache.keys())], max_cc)

        if len(cache)>20:
            cache.popitem(last=True)

        if values[-1] == exit_signal:
            logger.info("Optimizer Exits ...")
            break

        if len(ccs) == 1:
            ccs.append(2)
        else:
            difference = ccs[-1] - ccs[-2]
            prev, curr = values[-2], values[-1]
            if difference != 0 and prev !=0:
                gradient = (curr - prev)/(difference*prev)
            else:
                gradient = (curr - prev)/prev if prev != 0 else 1

            update_cc = ccs[-1] * gradient
            ## +- 5 : limit fluctuations
            if update_cc>0:
                update_cc = min(max(1, int(np.round(update_cc))), cc_change_limit)
            else:
                update_cc = max(min(-1, int(np.round(update_cc))), -cc_change_limit)

            ccs.append(min(max(ccs[-1] + update_cc, 1), soft_limit))
            logger.debug(f"Gradient: {gradient}")
            logger.info(f"Previous CC: {ccs[-2]}")

    return [ccs[-1]]

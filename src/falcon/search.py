from skopt.space import Integer
from skopt import Optimizer as Bayes
import numpy as np
import time

class Optimizer:
    def __init__(self, configurations, black_box_function, logger, verbose=True) -> None:
        self.configurations = configurations
        self.black_box_function = black_box_function
        self.logger = logger
        self.verbose = verbose


    def bayes_opt(self):
        limit_obs, count = 25, 0
        max_thread = self.configurations["thread_limit"]
        iterations = self.configurations["bayes"]["num_of_exp"]
        search_space  = [
            Integer(1, max_thread), # Concurrency
        ]

        params = []
        optimizer = Bayes(
            dimensions=search_space,
            base_estimator="GP", #[GP, RF, ET, GBRT],
            acq_func="gp_hedge", # [LCB, EI, PI, gp_hedge]
            acq_optimizer="auto", #[sampling, lbfgs, auto]
            n_random_starts=self.configurations["bayes"]["initial_run"],
            model_queue_size= limit_obs,
            # acq_func_kwargs= {},
            # acq_optimizer_kwargs={}
        )

        while True:
            count += 1

            if len(optimizer.yi) > limit_obs:
                optimizer.yi = optimizer.yi[-limit_obs:]
                optimizer.Xi = optimizer.Xi[-limit_obs:]

            if self.verbose:
                self.logger.info("Iteration {0} Starts ...".format(count))

            t1 = time.time()
            res = optimizer.run(func=self.black_box_function, n_iter=1)
            t2 = time.time()

            if self.verbose:
                self.logger.info("Iteration {0} Ends, Took {3} Seconds. Best Params: {1} and Score: {2}.".format(
                    count, res.x, res.fun, np.round(t2-t1, 2)))

            last_value = optimizer.yi[-1]
            if last_value == 10 ** 10:
                self.logger.info("Optimizer Exits ...")
                break

            cc = optimizer.Xi[-1][0]
            if iterations < 1:
                reset = False
                if (last_value > 0) and (cc < max_thread):
                    max_thread = max(cc, 2)
                    reset = True

                if (last_value < 0) and (cc == max_thread) and (cc < self.configurations["thread_limit"]):
                    max_thread = min(cc+5, self.configurations["thread_limit"])
                    reset = True

                if reset:
                    search_space[0] = Integer(1, max_thread)
                    optimizer = Optimizer(
                        dimensions=search_space,
                        n_initial_points=self.configurations["bayes"]["initial_run"],
                        acq_optimizer="lbfgs",
                        model_queue_size= limit_obs
                    )

            if iterations == count:
                self.logger.info("Best parameters: {0} and score: {1}".format(res.x, res.fun))
                params = res.x
                break

        return params


    def hill_climb(self):
        max_thread = self.configurations["thread_limit"]
        params = [1]
        phase, count = 1, 0
        current_value, previous_value = 0, 0

        while True:
            count += 1

            if self.verbose:
                self.logger.info("Iteration {0} Starts ...".format(count))

            t1 = time.time()
            current_value = self.black_box_function(params) * (-1)
            t2 = time.time()

            if self.verbose:
                self.logger.info("Iteration {0} Ends, Took {3} Seconds. Best Params: {1} and Score: {2}.".format(
                    count, params, current_value, np.round(t2-t1, 2)))

            if current_value == 10 ** 10:
                self.logger.info("Optimizer Exits ...")
                break

            if phase == 1:
                if (current_value > previous_value):
                    params[0] = min(max_thread, params[0]+1)
                    previous_value = current_value
                else:
                    params[0] = max(1, params[0]-1)
                    phase = 0

            elif phase == -1:
                if (current_value > previous_value):
                    params[0] = min(max_thread, params[0]+1)
                    phase = 0
                else:
                    params[0] = max(1, params[0]-1)
                    previous_value = current_value

            else:
                change = (current_value-previous_value)/previous_value
                previous_value = current_value
                if change > 0.1:
                    phase = 1
                    params[0] = min(max_thread, params[0]+1)
                elif change < -0.1:
                    phase = -1
                    params[0] = max(1, params[0]-1)

        return params


    def brute_force(self):
        score = []
        max_thread = self.configurations["thread_limit"]

        for i in range(1, max_thread+1):
            if self.verbose:
                self.logger.info("Iteration {0} Starts ...".format(i))

            score.append(self.black_box_function([i]))

            if score[-1] == 10 ** 10:
                break


        cc = np.argmin(score) + 1
        self.logger.info("Best parameters: {0} and score: {1}".format([cc], score[cc-1]))
        return [cc]


    def run_probe(self, current_cc, count):
        if self.verbose:
            self.logger.info("Iteration {0} Starts ...".format(count))

        t1 = time.time()
        current_value = self.black_box_function([current_cc])
        t2 = time.time()

        if self.verbose:
            self.logger.info("Iteration {0} Ends, Took {1} Seconds. Score: {2}.".format(
                count, np.round(t2-t1, 2), current_value))

        return current_value


    def gradient_opt(self):
        max_thread, count = self.configurations["thread_limit"], 0
        soft_limit, least_cost = max_thread, 0
        values = []
        ccs = [2]
        theta = 0

        while True:
            values.append(self.run_probe(ccs[-1]-1, count+1))
            if values[-1] == 10 ** 10:
                self.logger.info("Optimizer Exits ...")
                break

            if values[-1] < least_cost:
                least_cost = values[-1]
                soft_limit = min(ccs[-1]+10, max_thread)

            values.append(self.run_probe(ccs[-1]+1, count+2))
            if values[-1] == 10 ** 10:
                self.logger.info("Optimizer Exits ...")
                break

            if values[-1] < least_cost:
                least_cost = values[-1]
                soft_limit = min(ccs[-1]+10, max_thread)

            count += 2

            gradient = (values[-1] - values[-2])/2
            gradient_change = np.abs(gradient/values[-2])

            if gradient>0:
                if theta <= 0:
                    theta -= 1
                else:
                    theta = -1

            else:
                if theta >= 0:
                    theta += 1
                else:
                    theta = 1

            update_cc = int(theta * np.ceil(ccs[-1] * gradient_change))
            next_cc = min(max(ccs[-1] + update_cc, 2), soft_limit-1)
            self.logger.info("Gradient: {0}, Gredient Change: {1}, Theta: {2}, Previous CC: {3}, Choosen CC: {4}".format(gradient, gradient_change, theta, ccs[-1], next_cc))
            ccs.append(next_cc)

        return [ccs[-1]]


    def gradient_opt_fast(self):
        max_thread, count = self.configurations["thread_limit"], 0
        soft_limit, least_cost = max_thread, 0
        values = []
        ccs = [1]
        theta = 0

        while True:
            count += 1
            values.append(self.run_probe(ccs[-1], count))

            if values[-1] == 10 ** 10:
                self.logger.info("Optimizer Exits ...")
                break

            if values[-1] < least_cost:
                least_cost = values[-1]
                soft_limit = min(ccs[-1]+10, max_thread)

            if len(ccs) == 1:
                ccs.append(2)

            else:
                dist = max(1, np.abs(ccs[-1] - ccs[-2]))
                if ccs[-1]>ccs[-2]:
                    gradient = (values[-1] - values[-2])/dist
                else:
                    gradient = (values[-2] - values[-1])/dist

                if values[-2] !=0:
                    gradient_change = np.abs(gradient/values[-2])
                else:
                    gradient_change = np.abs(gradient)

                if gradient>0:
                    if theta <= 0:
                        theta -= 1
                    else:
                        theta = -1

                else:
                    if theta >= 0:
                        theta += 1
                    else:
                        theta = 1


                update_cc = int(theta * np.ceil(ccs[-1] * gradient_change))
                next_cc = min(max(ccs[-1] + update_cc, 2), soft_limit)
                self.logger.info("Gradient: {0}, Gredient Change: {1}, Theta: {2}, Previous CC: {3}, Choosen CC: {4}".format(gradient, gradient_change, theta, ccs[-1], next_cc))
                ccs.append(next_cc)

        return [ccs[-1]]

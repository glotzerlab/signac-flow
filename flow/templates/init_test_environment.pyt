#! /usr/bin/env python3
import signac

if __name__ == "__main__":
    pr = signac.init_project()
    for a in range({{num_jobs}}):
        pr.open_job({"a": a, "even": a % 2 == 0}).init()

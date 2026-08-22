package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"ydbgo/internal/proto"
)

func benchUpDel(args []string) {
	fs := flag.NewFlagSet("updel", flag.ExitOnError)
	addr := fs.String("addr", "localhost:2235", "coordinator addr")
	table := fs.String("table", "wshard8", "table")
	conc := fs.Int("c", 8, "concurrency")
	only := fs.String("only", "", "run only a single step: updatepoint|deletepoint|updaterange|deleterange")
	fs.Parse(args)
	pool := proto.NewConnPool(*conc)
	defer pool.Close()

	must := func(q string) {
		if err := pool.Do(*addr, func(c *proto.Client) error {
			resp, err := c.Execute(q)
			if err != nil {
				return err
			}
			if !resp.OK {
				return fmt.Errorf("%s", resp.Error)
			}
			return nil
		}); err != nil {
			fmt.Fprintln(os.Stderr, "err:", err)
			os.Exit(1)
		}
	}

	must("SELECT 1")

	run := func(name, q string, n int64) {
		var next int64
		var wg sync.WaitGroup
		worker := func() {
			defer wg.Done()
			for {
				stmt := atomic.AddInt64(&next, 1) - 1
				if stmt >= n {
					return
				}
				if err := pool.Do(*addr, func(c *proto.Client) error {
					resp, err := c.Execute(q)
					if err != nil {
						return err
					}
					if !resp.OK {
						return fmt.Errorf("%s", resp.Error)
					}
					return nil
				}); err != nil {
					fmt.Fprintln(os.Stderr, "err:", err)
					os.Exit(1)
				}
			}
		}
		start := time.Now()
		for i := 0; i < *conc; i++ {
			wg.Add(1)
			go worker()
		}
		wg.Wait()
		el := time.Since(start)
		fmt.Printf("%s: %d ops in %v (%.0f ops/s)\n", name, n, el, float64(n)/el.Seconds())
	}

	must(fmt.Sprintf("UPDATE %s SET g = 7 WHERE id = 42", *table))
	if *only == "" || *only == "updatepoint" {
		run("UPDATE point id=42", fmt.Sprintf("UPDATE %s SET g = 7 WHERE id = 42", *table), 200)
		run("UPDATE point id=750001", fmt.Sprintf("UPDATE %s SET g = 7 WHERE id = 750001", *table), 200)
	}
	if *only == "" || *only == "deletepoint" {
		run("DELETE point id=42", fmt.Sprintf("DELETE FROM %s WHERE id = 42", *table), 200)
		must(fmt.Sprintf("INSERT INTO %s VALUES (42,'v42',42)", *table))
	}
	if *only == "" || *only == "updaterange" {
		run("UPDATE range id<100000", fmt.Sprintf("UPDATE %s SET g = 7 WHERE id >= 0 AND id < 100000", *table), 20)
	}
	if *only == "" || *only == "deleterange" {
		run("DELETE range id<100000", fmt.Sprintf("DELETE FROM %s WHERE id >= 0 AND id < 100000", *table), 20)
	}
}

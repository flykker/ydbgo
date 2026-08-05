package server

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"ydbgo/internal/proto"
	"ydbgo/internal/sql"
)

// Dial opens a connection to a server.
func Dial(addr string) (*proto.Client, error) { return proto.Dial(addr) }

// RunClient executes SQL from args or an interactive REPL.
func RunClient(addr string, args []string) error {
	c, err := Dial(addr)
	if err != nil {
		return err
	}
	defer c.Close()
	if len(args) == 0 {
		interactive(c)
		return nil
	}
	// collect all args into a single SQL string
	if len(args) == 1 && strings.HasPrefix(args[0], "@") {
		return runBatch(c, strings.TrimPrefix(args[0], "@"))
	}
	q := strings.Join(args, " ")
	trimmed := strings.TrimSpace(q)
	resp, err := c.Execute(trimmed)
	if err != nil {
		return err
	}
	if !resp.OK {
		fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
		os.Exit(1)
	}
	tab := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	printResponse(tab, resp)
	return nil
}

// interactive runs a REPL reading from os.Stdin.
func interactive(c *proto.Client) {
	sc := bufio.NewScanner(os.Stdin)
	tab := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	for {
		fmt.Print("ydbgo> ")
		if !sc.Scan() {
			return
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if line == "\\q" || line == "exit" || line == "quit" {
			return
		}
		resp, err := c.Execute(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			continue
		}
		printResponse(tab, resp)
	}
}

// runBatch executes statements from a file, one at a time.
func runBatch(c *proto.Client, filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	stmts, err := sql.SplitStatements(string(data))
	if err != nil {
		return err
	}
	tab := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	defer tab.Flush()
	for _, st := range stmts {
		trimmed := strings.TrimSpace(st)
		if trimmed == "" {
			continue
		}
		resp, err := c.Execute(trimmed)
		if err != nil {
			return fmt.Errorf("executing %q: %w", trimmed, err)
		}
		if !resp.OK {
			return fmt.Errorf("executing %q: %s", trimmed, resp.Error)
		}
		printResponse(tab, resp)
	}
	return nil
}

func printResponse(tab *tabwriter.Writer, resp *proto.Response) {
	if !resp.OK {
		fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
		return
	}
	r := resp.Result
	if r == nil {
		fmt.Println("OK")
		return
	}
	switch r.Type {
	case "select":
		if len(r.Columns) == 0 {
			fmt.Println("OK")
			return
		}
		for _, c := range r.Columns {
			fmt.Fprintf(tab, "%s\t", c)
		}
		fmt.Fprintln(tab)
		for _, row := range r.Rows {
			for _, v := range row {
				fmt.Fprintf(tab, "%s\t", v)
			}
			fmt.Fprintln(tab)
		}
		fmt.Fprintf(tab, "(%d rows)\t\n", len(r.Rows))
	case "admin":
		if r.Note != "" {
			fmt.Println(r.Note)
		} else {
			fmt.Println("OK (admin)")
		}
	case "create_table", "drop_table", "create_index", "drop_index", "create_database":
		fmt.Printf("OK (%s)\n", r.Type)
	default:
		fmt.Printf("OK, %d row(s) affected\n", r.Affected)
	}
	tab.Flush()
}

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

var (
	Printf   = fmt.Fprintf
	Printfln = func(wr io.Writer, format string, args ...interface{}) { fmt.Fprintf(wr, format+"\n", args...) }
	Print    = fmt.Fprint
)

func main() {
	if err := run(os.Args, os.Stdout); err != nil {
		os.Exit(1)
	}
}

func run(args []string, wr io.Writer) error {
	rd := bufio.NewReader(os.Stdin)
	for {
		Print(wr, "db > ")
		in, err := rd.ReadString('\n')
		switch err {
		case nil:
		case io.EOF:
			return nil
		default:
			return err
		}

		in = strings.Trim(in, "\n")
		if in == "" {
			continue
		}
		if in[0] == '.' {
			switch doMetaCommand(in) {
			case MetaCommandAbort:
				return nil // exit loop
			case MetaCommandSuccess:
				continue
			case MetaCommandUnrecognizedCommand:
				Printf(wr, "Unrecognized command: (%s)\n", in)
				continue
			}
		}

		stmt := Statement{}
		switch prepareStatement(in, &stmt) {
		default: // PrepareUnrecognizedCharacter
			Printfln(wr, "Unrecognized statement (%s)", in)
			continue
		case PrepareResultSuccess:
			executeStatement(wr, stmt)
			Print(wr, "Executed.\n")
		}
	}
}

func executeStatement(wr io.Writer, stmt Statement) {
	switch stmt.Kind {
	case StatementKindInsert:
		Printfln(wr, "do insert statement")
	case StatementKindSelect:
		Printfln(wr, "do select statement")
	}
}

type MetaCommand uint

const (
	MetaCommandAbort MetaCommand = iota + 1
	MetaCommandSuccess
	MetaCommandUnrecognizedCommand
)

func doMetaCommand(in string) MetaCommand {
	switch in {
	case ".exit":
		return MetaCommandAbort
	default:
		return MetaCommandUnrecognizedCommand
	}
}

type PrepareResult uint

const (
	PrepareStatementUnrecognized PrepareResult = iota + 1
	PrepareResultSuccess
)

type Statement struct {
	Kind StatementKind
}

type StatementKind uint

const (
	StatementKindUnknown StatementKind = iota + 1
	StatementKindInsert
	StatementKindSelect
)

func prepareStatement(in string, stmt *Statement) PrepareResult {
	if strings.HasPrefix(in, "insert") {
		stmt.Kind = StatementKindInsert
		return PrepareResultSuccess
	}

	if strings.HasPrefix(in, "select") {
		stmt.Kind = StatementKindSelect
		return PrepareResultSuccess
	}

	return PrepareStatementUnrecognized
}

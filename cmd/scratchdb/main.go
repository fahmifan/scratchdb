package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"unsafe"
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

// run the repl
func run(args []string, wr io.Writer) error {
	rd := bufio.NewReader(os.Stdin)
	table := &Table{}
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
		case PrepareResultSyntaxError:
			Printfln(wr, "Syntax error")
		case PrepareResultSuccess:
			executeStatement(wr, stmt, table)
			Print(wr, "Executed\n")
		}
	}
}

const (
	IDSize                = uint32(unsafe.Sizeof(Row{}.ID))
	UsernameSize          = uint32(unsafe.Sizeof(Row{}.Username))
	EmailSize             = uint32(unsafe.Sizeof(Row{}.Email))
	IDOffset       uint32 = 0
	UsernameOffset        = IDOffset + IDSize
	EmailOffset           = UsernameOffset + UsernameSize
	RowSize               = IDSize + UsernameSize + EmailSize
	TableMaxPages  uint32 = 4096 // 4KB
	PageSize       uint32 = 4096 // 4KB
	RowsPerPage           = PageSize / IDSize
	TableMaxRows          = RowsPerPage * TableMaxPages
)

type Table struct {
	NumRows uint32
	Pages   [TableMaxPages][]byte
}

func rowSlot(table *Table, rowNum uint32) (page []byte, slot uint32) {
	pageNum := rowNum / RowsPerPage
	if table.Pages[pageNum] == nil {
		table.Pages[pageNum] = make([]byte, PageSize)
	}
	rowOffset := rowNum % RowsPerPage
	bytesOffset := rowOffset * RowSize
	return table.Pages[pageNum], bytesOffset
}

func executeStatement(wr io.Writer, stmt Statement, table *Table) {
	switch stmt.Kind {
	case StatementKindInsert:
		executeInsert(&stmt, table)
	case StatementKindSelect:
		executeSelect(&stmt, table)
	}
}

type ExecuteResult int

const (
	ExecuteTableFull ExecuteResult = iota + 1
	ExecuteSuccess
)

func serializeRow(row *Row, page []byte, slot uint32) {
	binary.BigEndian.PutUint32(page[slot+IDOffset:], row.ID)
	copy(page[slot+UsernameOffset:slot+EmailOffset], []byte(row.Username))
	copy(page[slot+EmailOffset:slot+RowSize], []byte(row.Email))
}

func deserializeRow(page []byte, slot uint32, row *Row) {
	row.ID = binary.BigEndian.Uint32(page[slot+IDOffset:])
	row.Username = string(trimNilBuf(page[slot+UsernameOffset : slot+EmailOffset]))
	row.Email = string(trimNilBuf(page[slot+EmailOffset : slot+RowSize]))
}

func trimNilBuf(buf []byte) []byte {
	const trimSet = "\x00"
	return bytes.Trim(buf, trimSet)
}

func executeInsert(stmt *Statement, table *Table) ExecuteResult {
	if table.NumRows >= TableMaxRows {
		return ExecuteTableFull
	}

	rowToInsert := &stmt.RowToInsert
	page, slot := rowSlot(table, table.NumRows)
	serializeRow(rowToInsert, page, slot)
	table.NumRows += 1

	return ExecuteSuccess
}

func executeSelect(stmt *Statement, table *Table) ExecuteResult {
	for i := uint32(0); i < table.NumRows; i++ {
		row := Row{}
		buf, slot := rowSlot(table, i)
		deserializeRow(buf, slot, &row)
		fmt.Println("row ", i, dump(row))
	}
	return ExecuteSuccess
}

type MetaCommand uint32

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

type PrepareResult uint32

const (
	PrepareStatementUnrecognized PrepareResult = iota + 1
	PrepareResultSyntaxError
	PrepareResultSuccess
)

type Row struct {
	ID       uint32
	Username string
	Email    string
}

func (r Row) Validate() bool {
	if r.Email == "" || r.Username == "" {
		return false
	}

	return false
}

type Statement struct {
	Kind        StatementKind
	RowToInsert Row
}

type StatementKind uint32

const (
	StatementKindUnknown StatementKind = iota + 1
	StatementKindInsert
	StatementKindSelect
)

func prepareStatement(in string, stmt *Statement) PrepareResult {
	if strings.HasPrefix(in, "insert") {
		stmt.Kind = StatementKindInsert
		nrow, err := fmt.Sscanf(in, "insert %d %s %s", &stmt.RowToInsert.ID, &stmt.RowToInsert.Username, &stmt.RowToInsert.Email)
		if err != nil || nrow != 3 {
			return PrepareResultSyntaxError
		}
		return PrepareResultSuccess
	}

	if strings.HasPrefix(in, "select") {
		stmt.Kind = StatementKindSelect
		return PrepareResultSuccess
	}

	return PrepareStatementUnrecognized
}

func dumpPretty(i interface{}) string {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	enc.SetIndent("", "  ")
	_ = enc.Encode(i)
	return buf.String()
}

func dump(i interface{}) string {
	buf := bytes.NewBuffer(nil)
	_ = json.NewEncoder(buf).Encode(i)
	return buf.String()
}

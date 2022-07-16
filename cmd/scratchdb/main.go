package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
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
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// run the repl
func run(args []string, wr io.Writer) (err error) {
	table, err := openDB("scratch.db")
	if err != nil {
		return err
	}
	defer func() {
		fmt.Println("DEBUG >>> table >>> NumRows >>> ", table.NumRows)
		if err = table.Close(); err != nil {
			fmt.Println("Error: ", err)
			return
		}
	}()

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
	Pager   *Pager
}

func (t *Table) Close() error {
	defer t.Pager.File.Sync()
	defer t.Pager.File.Close()

	for i := uint32(0); i < t.NumRows; i++ {
		buf, slot := rowSlot(t, i)
		t.Pager.File.WriteAt(buf, int64(slot))
	}

	return nil
}

type Pager struct {
	File  *os.File
	Pages [TableMaxPages][]byte
}

func openDB(fileName string) (*Table, error) {
	pager, err := openPager(fileName)
	if err != nil {
		return nil, err
	}
	fstat, err := pager.File.Stat()
	if err != nil {
		return nil, err
	}
	numRows := uint32(fstat.Size()) / RowSize
	table := Table{Pager: pager, NumRows: numRows}
	return &table, nil
}

func openPager(fileName string) (*Pager, error) {
	dbFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	pager := Pager{File: dbFile}

	return &pager, nil
}

var ErrFail = errors.New("failure")

func getPage(pager *Pager, pageNum uint32) ([]byte, error) {
	if pageNum > TableMaxPages {
		return nil, ErrFail
	}

	if pager.Pages[pageNum] == nil {
		pager.Pages[pageNum] = make([]byte, PageSize)
		fstat, err := pager.File.Stat()
		fsize := uint32(fstat.Size())
		if err != nil {
			return nil, err
		}

		fmt.Println("DEBUG >>> fsize >>> ", fsize)
		numPages := fsize / PageSize
		if fsize%PageSize == 0 {
			numPages++
		}
		fmt.Println("DEBUG >>> numPages >>> ", numPages)

		if pageNum <= uint32(numPages) {
			whence := 0
			_, err := pager.File.Seek(int64(pageNum*PageSize), whence)
			if err != nil {
				return nil, err
			}

			_, err = pager.File.Read(pager.Pages[pageNum])
			if err != nil && err != io.EOF {
				return nil, err
			}

		}
	}

	return pager.Pages[pageNum], nil
}

func rowSlot(table *Table, rowNum uint32) (page []byte, slot uint32) {
	pageNum := rowNum / RowsPerPage
	page, err := getPage(table.Pager, pageNum)
	if err != nil {
		// TODO: handled later
		panic(err)
	}
	rowOffset := rowNum % RowsPerPage
	bytesOffset := rowOffset * RowSize
	return page, bytesOffset
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
	ExecuteFail
)

func serializeRow(row *Row, page []byte, slot uint32) {
	binary.LittleEndian.PutUint32(page[slot+IDOffset:], row.ID)
	copy(page[slot+UsernameOffset:slot+EmailOffset], []byte(row.Username))
	copy(page[slot+EmailOffset:slot+RowSize], []byte(row.Email))
}

func deserializeRow(page []byte, slot uint32, row *Row) {
	row.ID = binary.LittleEndian.Uint32(page[slot+IDOffset:])
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

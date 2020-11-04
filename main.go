package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"os"
)

// ConvertPretty will transform sql to elasticsearch dsl, and prettify the output json
func ConvertPretty(sql string) (dsl string, table string, err error) {
	dsl, table, err = Convert(sql)
	if err != nil {
		return dsl, table, err
	}

	var prettifiedDSLBytes bytes.Buffer
	err = json.Indent(&prettifiedDSLBytes, []byte(dsl), "", "  ")
	if err != nil {
		return "", table, err
	}

	return string(prettifiedDSLBytes.Bytes()), table, err
}

// Convert will transform sql to elasticsearch dsl string
func Convert(sql string) (dsl string, table string, err error) {
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		return "", "", err
	}

	//sql valid, start to handle
	switch stmt.(type) {
	case *sqlparser.Select:
		dsl, table, err = handleSelect(stmt.(*sqlparser.Select))
	case *sqlparser.Update:
		return handleUpdate(stmt.(*sqlparser.Update))
	case *sqlparser.Insert:
		return handleInsert(stmt.(*sqlparser.Insert))
	case *sqlparser.Delete:
		return handleDelete(stmt.(*sqlparser.Delete))
	}

	if err != nil {
		return "", "", err
	}

	return dsl, table, nil
}

func main() {

	if len(os.Args) < 2 {
		return
	}

	var sql = os.Args[1]
	dsl, index, _ := Convert(sql)
	// 打印 type 名称
	//fmt.Println(index)
	// 打印 dsl
	//fmt.Println(dsl)
	//fmt.Println()
	// 打印格式化后的 dsl
	//var result = pretty.Pretty([]byte(dsl))
	//fmt.Println(string(result))
	var result = "{\"index\": \"" + index + "\", \"dsl\": " + dsl + "}"
	fmt.Println(result)
}

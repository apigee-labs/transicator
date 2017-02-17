package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"strconv"
)

var dbURL, trowss, tscopess string
var db *sql.DB
var trows, tscopes int

func main() {

	flag.Parse()
	argsp := flag.Args()
	dbURL = argsp[0]
	trowss = argsp[1]
	tscopess = argsp[2]
	if dbURL == "" || trowss == "" || tscopess == "" {
		fmt.Println("Args missing. loadgen <path> <rows cnt> <scopes cnt>")
		os.Exit(0)
	}
	trows, _ = strconv.Atoi(trowss)
	tscopes, _ = strconv.Atoi(tscopess)

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		fmt.Printf("Set env TEST_PG_URL. ")
		fmt.Print("Unable to open %s. Err [%v]\n", dbURL, err)
		os.Exit(1)
	}
	_, err = db.Exec(tableSQL)
	if err != nil {
		fmt.Printf("Unable to create table. Err: [%v]\n", err)
		os.Exit(2)
	}

	insert, err := db.Prepare(`insert into table1 (
			column1,
			column2,
			_change_selector)
			values ($1, $2, $3)`)
	if err != nil || insert == nil {
		fmt.Printf("Unable to Insert table for scope \n")
		os.Exit(3)
	}
	defer insert.Close()

	for i := 0; i < tscopes; i++ {
		s1 := strconv.FormatInt(int64(i), 10)
		// 100 rows <value_scope_0_0 to value_scope_100_100>
		for j := 0; j < trows; j++ {
			s2 := strconv.FormatInt(int64(j), 10)
			_, err = insert.Exec(
				"value_"+
					s1+"_"+s2,
				"value",
				"scope_"+s1)
			if err != nil {
				fmt.Printf("Unable to Insert table for pk")
				fmt.Printf(" [%s]\n", "value_"+s1+"_"+s2)
				os.Exit(4)
			}
		}
		fmt.Printf("Inserted load in to DB for scope %s\n", "scope_"+s1)
	}
}

var tableSQL = `
  create table table1(
    column1 varchar primary key,
    column2 varchar,
    _change_selector varchar);
  alter table table1 replica identity full;
`

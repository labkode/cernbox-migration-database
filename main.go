package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type cliFlags struct {
	dbUsername string
	dbPassword string
	dbHost     string
	dbPort     int
	dbName     string
	dryRun     bool
	eosMGMURL  string
}

func parseFlags() *cliFlags {
	flags := &cliFlags{}
	flag.StringVar(&flags.dbUsername, "username", "", "The username to connect to the db")
	flag.StringVar(&flags.dbPassword, "password", "", "The password to connect to the db")
	flag.StringVar(&flags.dbHost, "host", "", "The host of the db")
	flag.IntVar(&flags.dbPort, "port", 0, "The port of the db")
	flag.StringVar(&flags.dbName, "dbname", "", "The name of the database")
	flag.BoolVar(&flags.dryRun, "dryrun", true, "With dry run enbaled the changes are not commited to the db")
	flag.StringVar(&flags.eosMGMURL, "eosmgmurl", "root://eospps-slave.cern.ch", "The EOS MGM URL")
	flag.Parse()
	return flags
}

type shareInfo struct {
	ID          int            `db:"id"`
	ShareType   int            `db:"share_type"`
	ShareWith   sql.NullString `db:"share_with"`
	UIDOwner    string         `db:"uid_owner"`
	Parent      sql.NullInt64  `db:"parent"`
	ItemType    sql.NullString `db:"item_type"`
	ItemSource  sql.NullString `db:"item_source"`
	ItemTarget  sql.NullString `db:"item_target"`
	FileSource  sql.NullInt64  `db:"file_source"`
	FileTarget  sql.NullString `db:"file_target"`
	Permissions string         `db:"permissions"`
	STime       int            `db:"stime"`
	Accepted    int            `db:"accepted"`
	Expiration  time.Time      `db:"expiration"`
	Token       sql.NullString `db:"token"`
	MailSend    int            `db:"mail_send"`
}

type sqlDriver struct {
	db *sqlx.DB
}

func newSQLDriver(flags *cliFlags) (*sqlDriver, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", flags.dbUsername, flags.dbPassword, flags.dbHost, flags.dbPort, flags.dbName)
	d, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &sqlDriver{db: d}, nil
}
func (d *sqlDriver) getAllShares() ([]shareInfo, error) {
	var entries []shareInfo
	err := d.db.Select(&entries, "SELECT id,share_type,item_source,item_target,file_source,file_target from oc_share where share_type=3 and item_type='file' ORDER BY id;")
	if err != nil {
		return nil, err
	}
	return entries, nil
}

type Metadata struct {
	Inode int64
	Path  string
}

func (d *sqlDriver) getMetadataFromEOS(ID int64) (*Metadata, error) {
	cmd := exec.Command("/usr/bin/eos", "-r", "0", "0", "file", "info", fmt.Sprintf("inode:%d", ID), "-m")
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintln(os.Stderr,string(output))
		return nil, err
	}
	kv := make(map[string]string)
	raw := string(output)
	partsBySpace := strings.Split(raw, " ") // we have [keylength.file=14 file=/eos/pps/proc/ container=3 ...}
	for _, p := range partsBySpace {
		partsByEqual := strings.Split(p, "=") // we have kv pairs like [ keylength.file 14]
		if len(partsByEqual) == 2 {
			kv[partsByEqual[0]] = partsByEqual[1]
		}
	}
	inodeInt64, err := strconv.ParseInt(kv["ino"], 10, 64)
	if err != nil {
		return nil, err
	}
	m := &Metadata{Inode: inodeInt64, Path: kv["file"]}
	return m, nil
}
func main() {
	flags := parseFlags()
	os.Setenv("EOS_MGM_URL", flags.eosMGMURL)
	d, err := newSQLDriver(flags)
	if err != nil {
		fmt.Fprintln(os.Stderr,err)
		os.Exit(1)
	}
	shares, err := d.getAllShares()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cannot get all shares because ", err)
		os.Exit(1)
	}
	for _, s := range shares {
		meta, err := d.getMetadataFromEOS(s.FileSource.Int64)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		} else {
			fmt.Printf("id:%d share_type:%d item_source:%s item_target:%s file_source:%d file_target:%s eospath:%s\n", s.ID, s.ShareType, s.ItemSource.String, s.ItemTarget.String, s.FileSource.Int64, s.FileTarget.String, strconv.Quote(meta.Path))

		}
	}

}

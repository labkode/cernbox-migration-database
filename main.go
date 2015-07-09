package main

import (
	"flag"
	"fmt"
	"os"
	"time"

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
}

func parseFlags() *cliFlags {
	flags := &cliFlags{}
	flag.StringVar(&flags.dbUsername, "username", "", "The username to connect to the db")
	flag.StringVar(&flags.dbPassword, "password", "", "The password to connect to the db")
	flag.StringVar(&flags.dbHost, "host", "", "The host of the db")
	flag.IntVar(&flags.dbPort, "port", 0, "The port of the db")
	flag.StringVar(&flags.dbName, "dbname", "", "The name of the database")
	flag.BoolVar(&flags.dryRun, "dryrun", true, "With dry run enbaled the changes are not commited to the db")
	flag.Parse()
	return flags
}

type shareInfo struct {
	ID          int       `db:"id"`
	ShareType   string    `db:"share_type"`
	ShareWith   string    `db:"share_with"`
	UIDOwner    string    `db:"uid_owner"`
	Parent      int       `db:"parent"`
	ItemType    string    `db:"item_type"`
	ItemSource  string    `db:item_source`
	ItemTarget  string    `db:item_target`
	FileSource  string    `db:file_source`
	FileTarget  string    `db:file_target`
	Permissions string    `db:permissions`
	STime       int       `db:stime`
	Accepted    int       `db:accepted`
	Expiration  time.Time `db:expiration`
	Token       string    `db:token`
	MailSend    int       `db:mail_send`
}

/*
	id          | int(11)             | NO   | PRI | NULL    | auto_increment |
| share_type  | smallint(6)         | NO   |     | 0       |                |
| share_with  | varchar(255)        | YES  |     | NULL    |                |
| uid_owner   | varchar(64)         | NO   |     |         |                |
| parent      | int(11)             | YES  |     | NULL    |                |
| item_type   | varchar(64)         | NO   | MUL |         |                |
| item_source | varchar(255)        | YES  |     | NULL    |                |
| item_target | varchar(255)        | YES  |     | NULL    |                |
| file_source | bigint(20) unsigned | YES  | MUL | NULL    |                |
| file_target | varchar(512)        | YES  |     | NULL    |                |
| permissions | smallint(6)         | NO   |     | 0       |                |
| stime       | bigint(20)          | NO   |     | 0       |                |
| accepted    | smallint(6)         | NO   |     | 0       |                |
| expiration  | datetime            | YES  |     | NULL    |                |
| token       | varchar(32)         | YES  | MUL | NULL    |                |
| mail_send
	entry := &shareInfo{}
	err := l.db.QueryRowx("SELECT * FROM links WHERE token = ?", token).StructScan(entry)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, &storage.NotExistError{err.Error()}
		}
		return nil, err
	}

	linfo := &link.LinkInfo{}
	linfo.AuthID = entry.AuthID
	linfo.Username = entry.Username
	linfo.URI = entry.URI
	linfo.ResourceID = entry.ResourceID
	linfo.Resolution = entry.Resolution
	linfo.Extra = nil

	return linfo, nil
*/
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
	err := d.db.Select(&entries, "SELECT * FROM oc_share")
	if err != nil {
		return nil, err
	}
	return entries, nil
}
func main() {
	flags := parseFlags()
	d, err := newSQLDriver(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	shares, err := d.getAllShares()
	if err != nil {
		fmt.Println("Cannot get all shares because ", err)
		os.Exit(1)
	}
	for _, s := range shares {
		fmt.Printf("id: %s item_source:%s item_target: %s", s.ID, s.ItemSource, s.ItemTarget)
	}

}

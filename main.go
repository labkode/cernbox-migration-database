package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
	"bytes"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const VERSIONS_PREFIX = ".sys.v#."
var GLOBAL_FLAGS *cliFlags

type cliFlags struct {
	dbUsername string
	dbPassword string
	dbHost     string
	dbPort     int
	dbName     string
	dryRun     bool
	eosMGMURL  string
	debug bool
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
	flag.BoolVar(&flags.debug, "debug", false, "Print debug information")
	flag.Parse()
	GLOBAL_FLAGS = flags
	return flags
}

type shareInfo struct {
	ID          int64          `db:"id"`
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
	UID   string
	GID   string
}

func (d *sqlDriver) executeCMD(cmd *exec.Cmd) (string, string, error) {
	outBuf := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.Stdout = outBuf
	cmd.Stderr = errBuf
	err := cmd.Run()
	if GLOBAL_FLAGS.debug {
		fmt.Printf("DEBUG CMD: %+v ERR:%s", cmd,  err)
	}
	return outBuf.String(), errBuf.String(), err
}

// getMetadataFromEos returns the metadata fo the file/folder given the inode
func (d *sqlDriver) getMetadataFromEOS(ID int64) (*Metadata, error) {
	cmd := exec.Command("/usr/bin/eos", "-r", "0", "0", "file", "info", fmt.Sprintf("inode:%d", ID), "-m")
	stdout, _, err := d.executeCMD(cmd)
	if err != nil {
		return nil, err
	}
	return d.parseFileInfo(stdout)
}

// getMetadataFromEosPath returns the metadata fo the file/folder given the eos path
func (d *sqlDriver) getMetadataFromEOSPath(path string) (*Metadata, error) {
	cmd := exec.Command("/usr/bin/eos", "-r", "0", "0", "file", "info", path, "-m")
	stdout, _, err := d.executeCMD(cmd)
	if err != nil {
		return nil, err
	}
	return d.parseFileInfo(stdout)
}

// getVersionsFolderMetadata returns the metadata associated to the versions folder of the file with the inode passed
// if the versions folder does not exists it will try to create it.
func (d *sqlDriver) getVersionsFolderMetadata(fileMeta *Metadata) (*Metadata, error) {
	// obtain the versions folder path from the file path
	dirName := path.Dir(fileMeta.Path)
	baseName := path.Base(fileMeta.Path)
	versionsPath := path.Join(dirName, VERSIONS_PREFIX+baseName)
	versionsMeta, err := d.getMetadataFromEOSPath(versionsPath)
	if err != nil {
		// if versions folder does not exists (eos err code == 2) trigger creation of this folder
		if exiterr, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				if status.ExitStatus() == 2 {
					err = d.createVersionsFolder(fileMeta)
					if err != nil {
						return nil, err
					}
					
					// if we are talking to the slave the creation of the versions folder may not has been replicated, so we retry ever
					maxRetries := 5
					err := fmt.Errorf("Version not created yet")
					var versionsMeta *Metadata

					for maxRetries > 0 && err != nil {
						_versionsMeta, _err := d.getMetadataFromEOSPath(versionsPath)
						err = _err
						versionsMeta = _versionsMeta 
						maxRetries--

					}
					if err != nil {
						return nil, err
					}
					return versionsMeta, nil
				}
			}
			return nil, err
		}
		return nil, err
	}
	return versionsMeta, nil
}

func (d *sqlDriver) parseFileInfo(raw string) (*Metadata, error) {
	kv := make(map[string]string)
	partsBySpace := strings.Split(raw, " ") // we have [keylength.file=14 file=/eos/pps/proc/ container=3 ...}
	for _, p := range partsBySpace {
		partsByEqual := strings.Split(p, "=") // we have kv pairs like [ keylength.file 14]
		if len(partsByEqual) == 2 {
			kv[partsByEqual[0]] = partsByEqual[1]
		}
	}
	// fix eos path because the kv pair file=path could contains whitespace and the whitespace is the pair separator. Not very smart :(
	fileLength := kv["keylength.file"]
	fileLengthInt64, err := strconv.ParseInt(fileLength, 10, 64)
	if err != nil {
		return nil, err
	}
	startIndex := int64(14) + int64(len(fileLength)) + 7
	kv["file"] = raw[startIndex : startIndex+fileLengthInt64]

	inodeInt64, err := strconv.ParseInt(kv["ino"], 10, 64)
	if err != nil {
		return nil, err
	}
	m := &Metadata{Inode: inodeInt64, Path: kv["file"], UID: kv["uid"], GID: kv["gid"]}
	return m, nil
}
func (d *sqlDriver) createVersionsFolder(fileMeta *Metadata) error {
	cmd := exec.Command("/usr/bin/eos", "-r", fileMeta.UID, fileMeta.GID, "cp", fileMeta.Path, fileMeta.Path)
	_, stderr, err := d.executeCMD(cmd)
	if err != nil {
		fmt.Fprintln(os.Stderr, stderr)
		return err
	}
	return nil
}

func main() {
	flags := parseFlags()
	os.Setenv("EOS_MGM_URL", flags.eosMGMURL)
	d, err := newSQLDriver(flags)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	shares, err := d.getAllShares()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cannot get all shares because ", err)
		os.Exit(1)
	}
	if len(shares) == 0 {
		fmt.Fprintln(os.Stderr, "oc_share table does not contain public share files")
		os.Exit(1)
	}

	for _, s := range shares {
		meta, err := d.getMetadataFromEOS(s.FileSource.Int64)
		// TODO: filter already versions folder or ones that are not under your homedir like recycle files
		if err != nil {
			//fmt.Fprintln(os.Stderr, err)
		} else {
			fmt.Printf("info:file id:%d share_type:%d item_source:%s item_target:%s file_source:%d file_target:%s eospath:%s uid:%s gid:%s\n", s.ID, s.ShareType, s.ItemSource.String, s.ItemTarget.String, s.FileSource.Int64, s.FileTarget.String, strconv.Quote(meta.Path), meta.UID, meta.GID)
			versionsMeta, err := d.getVersionsFolderMetadata(meta)
			if err != nil {
				//fmt.Fprintln(os.Stderr, err)
			} else {
				fmt.Printf("info:versionfolder id:%d path:%s\n", versionsMeta.Inode, versionsMeta.Path)
			}
		}
	}

}

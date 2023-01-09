package main

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/tidwall/redcon"

	"github.com/Khighness/khighdb/database"
)

// @Author KHighness
// @Update 2023-01-09

var (
	config                  = new(ServerConfig)
	defaultDBPath           = filepath.Join("/tmp", "KHighDB")
	defaultHost             = "127.0.0.1"
	defaultPort        uint = 3333
	defaultDatabaseNum uint = 16
)

const (
	dbName = "KhighDB-%04d"
)

func init() {
	flag.StringVar(&config.dbPath, "dbpath", defaultDBPath, "the path of database directory")
	flag.StringVar(&config.host, "host", defaultHost, "server host")
	flag.UintVar(&config.port, "port", defaultPort, "server port")
	flag.UintVar(&config.databases, "databases", defaultDatabaseNum, "the number of databases")
	flag.Parse()
}

func main() {
	fmt.Println(banner())
	server := Start(config)
	go server.listen()
	<-server.sig
	server.stop()
}

// ServerConfig defines the structure of the server configuration.
type ServerConfig struct {
	dbPath    string
	host      string
	port      uint
	databases uint
}

// KhighDBServer defines the structure of the KhighDB server.
type KhighDBServer struct {
	dbs map[int]*khighdb.KhighDB
	svr *redcon.Server
	cfg *ServerConfig
	sig chan os.Signal
	mu  *sync.Mutex
}

// banner returns the string banner in file.
func banner() string {
	path, err := filepath.Abs("assets/banner.txt")
	if err != nil {
		panic(err)
	}
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return string(buf)
}

// Start initializes and returns a database server.
func Start(config *ServerConfig) *KhighDBServer {
	// Open the default database.
	timeStart := time.Now()
	path := filepath.Join(config.dbPath, fmt.Sprintf(dbName, 0))
	opts := khighdb.DefaultOptions(path)
	db0, err := khighdb.Open(opts)
	if err != nil {
		zap.S().Fatal("Failed to start server, open KhighDB error: %v", err)
	}
	dbs := make(map[int]*khighdb.KhighDB)
	dbs[0] = db0
	zap.S().Infof("Succeed to open KhighDB from [%v], time cost: %v", path, time.Since(timeStart))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// Initialize the tcp server.
	server := &KhighDBServer{
		dbs: dbs,
		cfg: config,
		sig: sig,
		mu:  new(sync.Mutex),
	}
	addr := fmt.Sprintf("%s:%v", config.host, config.port)
	svr := redcon.NewServerNetwork("tcp", addr, server.handle, server.accept, server.closed)
	server.svr = svr
	return server
}

// listen starts the database server to listen for connections.
func (server *KhighDBServer) listen() {
	zap.S().Infof("KhighDB server is listening on %s", fmt.Sprintf("%s:%v", config.host, config.port))
	if err := server.svr.ListenAndServe(); err != nil {
		zap.S().Fatalf("KhighDB server startup failed, error: %v", err)
	}
}

// stop stops the database server.
func (server *KhighDBServer) stop() {
	for i, db := range server.dbs {
		if err := db.Close(); err != nil {
			zap.S().Errorf("Close database[%d] error: %v", i, err)
		}
	}
	if err := server.svr.Close(); err != nil {
		zap.S().Errorf("Close KhighDB error: %v", err)
	}
	zap.S().Info("KhighDB is ready to exit, bye...")
}

// handle handles client command request.
func (server *KhighDBServer) handle(conn redcon.Conn, cmd redcon.Command) {
	// TODO
}

// accept handles client connection request.
func (server *KhighDBServer) accept(conn redcon.Conn) bool {
	// TODO
	zap.S().Infof("Accept connection from %s", conn.RemoteAddr())
	return true
}

// closed handles client connection close request.
func (server *KhighDBServer) closed(conn redcon.Conn, err error) {
	// TODO
	zap.S().Infof("Close connection with %s", conn.RemoteAddr())
}

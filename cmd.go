package mini_cockroachdb

import (
	"fmt"
	"github.com/spf13/cobra"
	bolt "go.etcd.io/bbolt"
	"log"
	"net/http"
	"os"
	"path"
)

var rootCmd = &cobra.Command{
	Use:   "mini_cockroachdb",
	Short: "mini_cockroachdb is a mini version cockroachdb implemented in pure go.",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		// Do stuff here
	},
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start server",
	Run: func(cmd *cobra.Command, args []string) {
		dataDir := "data"
		err := os.MkdirAll(dataDir, os.ModePerm)
		if err != nil {
			log.Fatalf("os.MkdirAll() failed, err: %s", err)
		}

		db, err := bolt.Open(path.Join(dataDir, "/data"+nodeId), 0600, nil)
		if err != nil {
			log.Fatalf("bolt.Open() failed, err: %s", err)
		}
		defer func(db *bolt.DB) {
			err := db.Close()
			if err != nil {

			}
		}(db)

		pe := newPgEngine(db)
		// start off in clean state
		err = pe.delete()
		if err != nil {
			log.Fatalf("pe.delete() failed, err: %s", err)
		}

		// setup raft server
		pf := &pgFsm{pe: pe}
		r, err := setupRaft(path.Join(dataDir, "raft"+nodeId), nodeId, fmt.Sprintf("localhost:%d", raftPort), pf)
		if err != nil {
			log.Fatalf("setupRaft() failed, err: %s", err)
		}

		// setup http server
		hs := httpServer{r: r}
		http.HandleFunc("/add_follower", hs.addFollowerHandler)
		go func() {
			err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil)
			if err != nil {
				log.Fatalf("http.ListenAndServe() failed, err: %s", err)
			}
		}()

		// run pg server
		runPgServer(pgPort, db, r)
	},
}

var (
	nodeId   string
	httpPort int
	raftPort int
	pgPort   int
)

func init() {
	startCmd.PersistentFlags().StringVarP(&nodeId, "node-id", "n", "", "node id")
	startCmd.PersistentFlags().IntVarP(&httpPort, "http-port", "h", 0, "http port")
	startCmd.PersistentFlags().IntVarP(&raftPort, "raft-port", "h", 0, "raft port")
	startCmd.PersistentFlags().IntVarP(&pgPort, "pg-port", "h", 0, "pg port")

	rootCmd.AddCommand(startCmd)
}

func Execute() error {
	return rootCmd.Execute()
}

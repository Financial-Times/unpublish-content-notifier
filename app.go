package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"

	tid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/jawher/mow.cli"
)

var client = &http.Client{}

var vulcanHost *string

func init() {
	f := &log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	}

	log.SetFormatter(f)
}

func main() {

	app := cli.App("unpublish-content-notifier", "notifies unpublished content")

	vulcanHost = app.String(cli.StringOpt{
		Name:   "vulcan-host",
		Value:  "localhost",
		Desc:   "vulcan host",
		EnvVar: "VULCAN_HOST",
	})

	app.Action = func() {
		r := mux.NewRouter()
		r.HandleFunc("/notify", forwardToTransformerAndSenttoS3).Methods("POST")
		r.HandleFunc("/__health", dummyHC).Methods("GET")
		http.Handle("/", r)
		log.Info("Starting server...")
		err := http.ListenAndServe(":8080", nil)
		if err != nil {
			log.WithError(err).Panic("Couldn't set up HTTP listener")
		}
	}
}

func forwardToTransformerAndSenttoS3(w http.ResponseWriter, r *http.Request) {
	transactionID := tid.GetTransactionIDFromRequest(r)
	var c content
	mesgBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithField("transaction_id", transactionID).WithError(err).Error("Error in parsing content body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = json.Unmarshal(mesgBody, &c); err != nil {
		log.WithField("transaction_id", transactionID).WithError(err).Error("Error in parsing content body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// call the mapper
	mapperReq, err := http.NewRequest("POST", "http://"+*vulcanHost+":8080/map?preview=true", bytes.NewReader(mesgBody))
	if err != nil {
		log.WithField("transaction_id", transactionID).WithError(err).Error("Error in creating request to mapper")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	mapperReq.Header.Set(tid.TransactionIDHeader, transactionID)
	mapperReq.Host = "methode-article-mapper"
	mapperReq.Header.Set("Content-Type", "application/json")

	mapperResp, err := client.Do(mapperReq)
	if err != nil {
		log.WithField("transaction_id", transactionID).WithError(err).Error("Error in calling mapper")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if mapperResp.StatusCode != http.StatusOK {
		http.Error(w, fmt.Sprintf("mapper returned status %v", mapperResp.StatusCode), http.StatusInternalServerError)
		return
	}

	// call the s3 writer
	s3WriterReq, err := http.NewRequest("PUT", "http://"+*vulcanHost+":8080/"+c.UUID, mapperResp.Body)
	if err != nil {
		log.WithField("transaction_id", transactionID).WithError(err).Error("Error in creating request to S3 writer")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s3WriterReq.Header.Set(tid.TransactionIDHeader, transactionID)
	s3WriterReq.Host = "content-rw-s3"
	s3WriterReq.Header.Set("Content-Type", "application/json")

	s3WriterResp, err := client.Do(s3WriterReq)
	if err != nil {
		log.WithField("transaction_id", transactionID).WithError(err).Error("Error in calling s3 writer")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if s3WriterResp.StatusCode != http.StatusCreated {
		http.Error(w, fmt.Sprintf("s3 writer returned status %v", s3WriterResp.StatusCode), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(fmt.Sprintf("Written content %v", c.UUID)))
}

type content struct {
	UUID string `json:"uuid"`
}

func dummyHC(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("YOLO"))
}

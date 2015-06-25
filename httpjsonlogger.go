package main

import (
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var (
	listenPort = flag.String("port", "8080", "port to listen on, defaults to 8080")
	PROG       = "httpjsonlogger"
	ERR        = "error"
	STATUS     = "status"
)

type Message struct {
	Source  *string                `json:"source"`
	Key     *string                `json:"key"`
	Content map[string]interface{} `json:"content"`
}

func logMessage(log *log.Logger, message *Message) {
	msg, err := json.Marshal(message)

	if err != nil {
		log.Printf("{ \"source\": \""+PROG+"\", \"key\": \""+ERR+"\", \"payload\": { \"error\": \"%s\"} }\n",
			fmt.Sprintf("error marshalling json: %s", err))
	} else {
		log.Println(string(msg))
	}
}

func logMissingParam(log *log.Logger, param string, msg *Message) {
	logMessage(log,
		&Message{
			Source: &PROG,
			Key:    &ERR,
			Content: map[string]interface{}{
				"error": fmt.Sprintf("missing '%s' param", param),
				"orig":  msg,
			},
		})
}

func idHandler(msgs *log.Logger, errs *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg *Message

		message, err := ioutil.ReadAll(r.Body)

		if err != nil {
			logMessage(errs,
				&Message{
					Source: &PROG,
					Key:    &ERR,
					Content: map[string]interface{}{
						"error": fmt.Sprintf("error reading body: %s", err),
					},
				})
			return
		}

		// Validate json message
		err = json.Unmarshal([]byte(message), &msg)
		if err != nil {
			logMessage(errs,
				&Message{
					Source: &PROG,
					Key:    &ERR,
					Content: map[string]interface{}{
						"error": fmt.Sprintf("error reading json: %s", err),
					},
				})
			return
		}

		if msg.Source == nil {
			logMissingParam(errs, "source", msg)
			return
		}

		if msg.Key == nil {
			logMissingParam(errs, "key", msg)
			return
		}

		if msg.Content == nil {
			logMissingParam(errs, "key", msg)
			return
		}

		msgs.Println(string(message))
	})
}

func main() {
	flag.Parse()
	expvar.NewString("listen_port").Set(*listenPort)

	stdout := log.New(os.Stdout, "", log.Ldate|log.Ltime)
	stderr := log.New(os.Stderr, "", log.Ldate|log.Ltime)

	http.Handle("/msg", idHandler(stdout, stderr))

	logMessage(stdout,
		&Message{
			Source: &PROG,
			Key:    &STATUS,
			Content: map[string]interface{}{
				"status": "listening",
				"port":   *listenPort,
			},
		})

	err := http.ListenAndServe(fmt.Sprintf(":%s", *listenPort), nil)
	if err != nil {
		logMessage(stderr,
			&Message{
				Source: &PROG,
				Key:    &ERR,
				Content: map[string]interface{}{
					"error": fmt.Sprintf("error starting to listen: %s", err),
				},
			})
		os.Exit(1)
	}
}

package main

import (
	"log"

	"github.com/michael-abb/commitlog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}

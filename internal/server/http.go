package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/mux"
)

type ( 
  API struct {
    Log *Log
  }

  ProduceRequest struct {
    Record Record `json:"record"`
  }

  ProduceResponse struct {
    Offset uint64 `json:"offset"`
  }

  ConsumeRequest struct {
    Offset uint64 `json:"offset"`
  }

  ConsumeResponse struct {
    Record Record `json:"record"`
  }
)

func (a *API) handleProduce(w http.ResponseWriter, r *http.Request) {
  var req ProduceRequest
  if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
    http.Error(w, err.Error(), http.StatusBadRequest) 
    return
  }
  off, err := a.Log.Append(req.Record)
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }
  res := ProduceResponse{Offset: off}
  if err := json.NewEncoder(w).Encode(res); err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }
}

func (a *API) handleConsume(w http.ResponseWriter, r *http.Request) {
  var req ConsumeRequest

  if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
    http.Error(w, err.Error(), http.StatusBadRequest)
    return
  }

  record, err := a.Log.Read(req.Offset)

  if errors.Is(err, ErrOffSetNotFound) {
    http.Error(w, err.Error(), http.StatusNotFound)
    return
  }

  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  res := ConsumeResponse{Record: record}
  if err := json.NewEncoder(w).Encode(res); err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }
}

func newAPI() *API {
  return &API{
    Log: NewLog(),
  }
}

func NewHTTPServer(addr string) *http.Server {
  api := newAPI()
 
  r := mux.NewRouter()
  r.HandleFunc("/", api.handleProduce).Methods("POST")
  r.HandleFunc("/", api.handleConsume).Methods("GET")
 
  return &http.Server{
    Addr: addr,
    Handler: r,
  }
}

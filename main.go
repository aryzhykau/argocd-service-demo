package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafka "github.com/segmentio/kafka-go"
)

var messagesConsumed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "kafka_messages_consumed_total",
	Help: "Total number of Kafka messages consumed and processed.",
})

type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type HelloResponse struct {
	Message string `json:"message"`
	Version string `json:"version"`
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(HealthResponse{
		Status:  "ok",
		Service: "argocd-service-demo",
	})
}

func helloHandler(w http.ResponseWriter, r *http.Request) {
	version := os.Getenv("APP_VERSION")
	if version == "" {
		version = "0.1.0"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(HelloResponse{
		Message: "Hello from argocd-service-demo!",
		Version: version,
	})
}

func runConsumer() {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")
	group := os.Getenv("KAFKA_CONSUMER_GROUP")
	delayMs := 200

	if brokers == "" || topic == "" || group == "" {
		log.Println("KAFKA_BROKERS / KAFKA_TOPIC / KAFKA_CONSUMER_GROUP not set — consumer disabled")
		return
	}

	if d := os.Getenv("PROCESS_DELAY_MS"); d != "" {
		if v, err := strconv.Atoi(d); err == nil {
			delayMs = v
		}
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokers},
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer r.Close()

	log.Printf("Consumer started: topic=%s group=%s delay=%dms", topic, group, delayMs)

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Consumer read error: %v — retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("Message received: partition=%d offset=%d key=%s",
			msg.Partition, msg.Offset, string(msg.Key))
		messagesConsumed.Inc()
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	go runConsumer()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/", helloHandler)
	mux.Handle("/metrics", promhttp.Handler())

	log.Printf("Starting server on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

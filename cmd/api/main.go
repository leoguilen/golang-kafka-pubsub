package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/leoguilen/golang-kafka-pubsub/configs"
	"github.com/leoguilen/golang-kafka-pubsub/internal/events"
	"github.com/leoguilen/golang-kafka-pubsub/pkg/kafka"
)

var _config *configs.Config

func init() {
	var err error
	if _config, err = configs.New(); err != nil {
		panic(fmt.Errorf("[INIT] - failed to load config: %w", err))
	}
}

func main() {
	bus, err := kafka.Inject(_config)
	if err != nil {
		panic(fmt.Errorf("[MAIN] - failed to inject kafka event bus: %w", err))
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/greetings", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		name := r.URL.Query().Get("name")
		if name == "" {
			name = "Anonymous"
		}

		correlationId := r.Header.Get("X-Correlation-ID")
		if correlationId == "" {
			correlationId = uuid.New().String()
		}

		var message string
		switch time.Now().Hour() {
		case 6, 7, 8, 9, 10, 11:
			message = "Good morning " + name + "!"
		case 12, 13, 14, 15, 16, 17:
			message = "Good afternoon " + name + "!"
		case 18, 19:
			message = "Good evening " + name + "!"
		default:
			message = "Good night " + name + "!"
		}

		event := events.NewGreetingReceivedEvent(message, map[string]interface{}{
			"source":         "go-api/v1",
			"environment":    _config.Env,
			"correlation-id": correlationId,
		})
		go bus.Publish(context.Background(), "greetings", event)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Correlation-ID", correlationId)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"message": message,
		})
	})

	fmt.Printf("[MAIN] - http server started on port %s\n", _config.Port)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", _config.Port), mux); err != nil {
		panic(fmt.Errorf("[MAIN] - failed to start http server: %w", err))
	}
}

package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"sync"

	"github.com/gorilla/websocket"
	_ "github.com/mattn/go-sqlite3"
)

// Coin represents the structure of each coin in the JSON response
type Coin struct {
	ID               int    `json:"id"`
	FullName         string `json:"full_name"`
	Coin             string `json:"coin"`
	BuyLimit         int    `json:"buy_limit"`
	SellLimit        int    `json:"sell_limit"`
	WithdrawalFee    string `json:"withdrawal_fee"`
	DepositFees      string `json:"deposit_fees"`
	Status           string `json:"status"`
	DepositStatus    string `json:"deposit_status"`
	WithdrawalStatus string `json:"withdrawal_status"`
	Icon             string `json:"icon"`
	Timestamp        string `json:"timestamp"`
}

// WebSocketSender sends each coin's data over WebSocket
func WebSocketSender(coin Coin, addr string, errCh chan<- error) {
	client := &websocket.Dialer{
	}

	conn, _, err := client.Dial(addr, nil)
	if err != nil {
		errCh <- err
		log.Printf("error connecting to WebSocket endpoint: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("connected to WebSocket server: %s", addr)

	// Marshal coin data to JSON
	data, err := json.Marshal(coin)
	if err != nil {
		errCh <- err
		log.Printf("error marshaling JSON: %v", err)
		return
	}

	// Write data to WebSocket connection
	err = conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		errCh <- err
		log.Printf("error writing message to WebSocket: %v", err)
		return
	}

	log.Printf("sent coin %s (%s)", coin.FullName, coin.Coin)
	errCh <- nil
}

func main() {
	// Open a connection to the SQLite database
	db, err := sql.Open("sqlite3", "./coins.db")
	if err != nil {
		log.Fatalf("error connecting to database: %v", err)
	}
	defer db.Close()

	// Ensure the coins table exists with a timestamp column
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS coins (
			id INTEGER,
			full_name TEXT,
			coin TEXT,
			buy_limit INTEGER,
			sell_limit INTEGER,
			withdrawal_fee TEXT,
			deposit_fees TEXT,
			status TEXT,
			deposit_status TEXT,
			withdrawal_status TEXT,
			icon TEXT,
			timestamp TEXT,
			PRIMARY KEY (id, timestamp)
		);
	`)
	if err != nil {
		log.Fatalf("error creating table: %v", err)
	}

	// Fetch coins from the database
	rows, err := db.Query(`SELECT id, full_name, coin, buy_limit, sell_limit, withdrawal_fee, deposit_fees, status, deposit_status, withdrawal_status, icon, timestamp FROM coins`)
	if err != nil {
		log.Fatalf("error querying coins: %v", err)
	}
	defer rows.Close()

	// Channel for error handling
	errCh := make(chan error, 1)

	// WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup

	// Process each row in the database
	for rows.Next() {
		var coin Coin
		if err := rows.Scan(&coin.ID, &coin.FullName, &coin.Coin, &coin.BuyLimit, &coin.SellLimit, &coin.WithdrawalFee, &coin.DepositFees, &coin.Status, &coin.DepositStatus, &coin.WithdrawalStatus, &coin.Icon, &coin.Timestamp); err != nil {
			log.Printf("error scanning row: %v", err)
			continue
		}

		wg.Add(1)
		go func(c Coin) {
			defer wg.Done()
			WebSocketSender(c, "wss://stream.bit24hr.in/coin_market_history/", errCh)
		}(coin)
	}

	// Close the error channel once all sending is done
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// Log any errors received from the WebSocketSender goroutines
	for err := range errCh {
		if err != nil {
			log.Printf("error occurred: %v", err)
		}
	}

	log.Println("All coins sent to WebSocket server")
}

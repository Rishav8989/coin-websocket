package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Coin represents the structure of each coin in the JSON data from the API
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
}

// APIResponse represents the structure of the API response
type APIResponse struct {
	Coins []Coin `json:"coins"`
}

func fetchAndStoreCoins(db *sql.DB) {
	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("Fetching data from API...")
			resp, err := client.Get("https://authentication.bit24hr.in/api/v1/get-coins")
			if err != nil {
				log.Println("Error fetching data:", err)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				log.Println("Error: non-200 status code received:", resp.StatusCode)
				resp.Body.Close()
				continue
			}

			var apiResponse APIResponse
			err = json.NewDecoder(resp.Body).Decode(&apiResponse)
			resp.Body.Close()
			if err != nil {
				log.Println("Error decoding JSON data:", err)
				continue
			}

			log.Println("Data fetched successfully. Storing in database...")
			timestamp := time.Now().Unix()

			for _, coin := range apiResponse.Coins {
				_, err := db.Exec("INSERT INTO coins (id, full_name, coin, buy_limit, sell_limit, withdrawal_fee, deposit_fees, status, deposit_status, withdrawal_status, icon, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					coin.ID, coin.FullName, coin.Coin, coin.BuyLimit, coin.SellLimit, coin.WithdrawalFee, coin.DepositFees, coin.Status, coin.DepositStatus, coin.WithdrawalStatus, coin.Icon, timestamp)
				if err != nil {
					log.Println("Error inserting data:", err)
				} else {
					log.Printf("Inserted coin: %d, %s, %s at %d\n", coin.ID, coin.Coin, coin.FullName, timestamp)
				}
			}

			log.Println("Data stored successfully.")
		}
	}
}

func main() {
	// Connect to the SQLite database
	db, err := sql.Open("sqlite3", "./new_coin.db")
	if err != nil {
		log.Fatal("Error connecting to database:", err)
	}
	defer db.Close()

	// Drop the coins table if it exists (for this example, to ensure correct schema)
	_, err = db.Exec(`DROP TABLE IF EXISTS coins`)
	if err != nil {
		log.Fatal("Error dropping table:", err)
	}

	// Create the coins table with the correct schema
	_, err = db.Exec(`CREATE TABLE coins (
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
		timestamp INTEGER
	)`)
	if err != nil {
		log.Fatal("Error creating table:", err)
	}

	log.Println("Database connected and table ensured. Starting data fetch routine...")

	// Start the routine to fetch and store coins every second
	fetchAndStoreCoins(db)
}

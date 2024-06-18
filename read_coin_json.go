package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
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

// GetCoinsFromDB retrieves all coins from the database within the last second
func GetCoinsFromDB(db *sql.DB) ([]Coin, error) {
	rows, err := db.Query(`SELECT id, full_name, coin, buy_limit, sell_limit, withdrawal_fee, deposit_fees, status, deposit_status, withdrawal_status, icon, timestamp FROM coins WHERE timestamp >= ? ORDER BY timestamp DESC`, time.Now().Add(-1*time.Second).Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("failed to query coins: %v", err)
	}
	defer rows.Close()

	var coins []Coin
	for rows.Next() {
		var coin Coin
		if err := rows.Scan(&coin.ID, &coin.FullName, &coin.Coin, &coin.BuyLimit, &coin.SellLimit, &coin.WithdrawalFee, &coin.DepositFees, &coin.Status, &coin.DepositStatus, &coin.WithdrawalStatus, &coin.Icon, &coin.Timestamp); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		coins = append(coins, coin)
	}
	return coins, nil
}

func main() {
	// Open a connection to the SQLite database
	db, err := sql.Open("sqlite3", "./coins.db")
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		return
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
			timestamp TEXT
		);
	`)
	if err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		return
	}

	// Create a new Gin router
	router := gin.Default()

	// Define a route to retrieve coins from the database
	router.GET("/get-coins", func(c *gin.Context) {
		// Retrieve the coins from the database
		coins, err := GetCoinsFromDB(db)
		if err != nil {
			// If there's an error, respond with a 500 status and the error message
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Respond with the coins data
		c.JSON(http.StatusOK, gin.H{"coins": coins})
	})

	// Start the ticker to fetch and send data every second
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			// Fetch the coins from the database
			coins, err := GetCoinsFromDB(db)
			if err != nil {
				fmt.Printf("Error fetching coins from database: %v\n", err)
				continue
			}

			// Print the fetched coins (for debugging purposes)
			fmt.Printf("Fetched coins: %+v\n", coins)
		}
	}()

	// Run the Gin server on port 8080
	router.Run(":8080")
}

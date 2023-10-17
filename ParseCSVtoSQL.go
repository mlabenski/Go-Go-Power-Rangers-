package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

// Assuming you have Product structure like this:
type Product struct {
	Category       string
	BrandID        int // Changed to BrandID
	Price          string
	NicotineAmount string
	BottleSize     string
	Description    string
	Flavor         string
}

func main() {
	// Open the CSV
	file, err := os.Open("productslemoyne.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a new reader.
	r := csv.NewReader(file)

	// Skip the header row.
	_, err = r.Read()
	if err != nil {
		log.Fatal(err)
	}

	// Open the DB
	db, err := sql.Open("sqlite3", "./products.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if len(record) != 7 {
			log.Printf("Skipping record, found %d fields, but expected 7", len(record))
			continue
		}
		// Retrieve brand ID based on brand name
		brandName := record[1]
		log.Printf(brandName)
		var brandID int
		err = db.QueryRow(`SELECT id FROM brands WHERE name = ?`, brandName).Scan(&brandID)
		if err != nil {
			log.Fatalf("Failed to get brand ID for %s: %v", brandName, err)
			continue
		}

		product := Product{
			Category:       record[0],
			BrandID:        brandID,
			Price:          record[2],
			NicotineAmount: record[3],
			BottleSize:     record[4],
			Description:    record[5],
			Flavor:         record[6],
		}

		// Insert the product into the database.
		_, err = db.Exec(`INSERT INTO products (category, brand_id, price, nicotine_amount, bottle_size, description, flavor) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			product.Category, product.BrandID, product.Price, product.NicotineAmount, product.BottleSize, product.Description, product.Flavor)
		if err != nil {
			log.Printf("Failed to insert record for %s: %v", brandName, err)
			// If there's an error inserting the record, we log it and proceed without exiting fatally.
			continue
		}
	}

	fmt.Println("Data import completed.")
}

func createTablesTwo(db *sql.DB) {
	// SQL statement for creating a new 'products' table
	sqlCreateProductsTable := `
	CREATE TABLE IF NOT EXISTS products (
		product_id INTEGER PRIMARY KEY AUTOINCREMENT,
		brand_id INTEGER,
		price INTEGER NOT NULL,
		nicotine_amount INTEGER NOT NULL,
		bottle_size_id INTEGER,
		description TEXT,
		flavor TEXT,
		category TEXT,
		FOREIGN KEY (brand_id) REFERENCES brands(brand_id)
		-- Add other foreign key constraints if needed
	);
	`

	// Execute the SQL statement for 'products' table
	_, err := db.Exec(sqlCreateProductsTable)
	if err != nil {
		log.Fatalf("Failed to create 'products' table: %s", err)
	}

	// SQL statement for creating a new 'store_products' junction table
	sqlCreateStoreProductsTable := `
	CREATE TABLE IF NOT EXISTS store_products (
		store_id INTEGER,
		product_id INTEGER,
		stock_count INTEGER NOT NULL CHECK (stock_count >= 0), -- ensures a non-negative inventory count
		PRIMARY KEY (store_id, product_id),
		FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
		FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
	);
	`

	// Execute the SQL statement for 'store_products' table
	_, err = db.Exec(sqlCreateStoreProductsTable)
	if err != nil {
		log.Fatalf("Failed to create 'store_products' table: %s", err)
	}

	log.Println("Tables created successfully!")
}

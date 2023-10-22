// This is the first step in normalizing the data
// We loop through each product as defined in .csv 
// Compare the brand name to the brands table.
// If a brand exists, do nothing
// If a brand doens't exist, create a entry for the brand name.

package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3" // Import SQLite3 driver
)

type Store struct {
	StoreID int    `json:"storeID"`
	Phone   string `json:"phone"`
	Address string `json:"address"`
}

type StoreProduct struct {
	StoreID    int    `json:"storeID"`
	ProductID  int    `json:"productID"`
	StockCount int    `json:"stockCount"`
	Categories string `json:"categories"`
}

type Brands struct {
	id   int
	name string
}

type Product struct {
	ProductID      int    `json:"product_id"` // corresponds to 'product_id' in the 'products' table
	BrandName      string `json:"brand_name"`
	BrandID        int    `json:"brand_id"`        // corresponds to 'brand_id' in the 'products' table
	Price          string `json:"price"`           // corresponds to 'price' in the 'products' table
	NicotineAmount string `json:"nicotine_amount"` // corresponds to 'nicotine_amount' in the 'products' table
	BottleSize     string `json:"bottle_size"`     // corresponds to 'bottle_size' in the 'products' table
	Description    string `json:"description"`     // corresponds to 'description' in the 'products' table
	Flavor         string `json:"flavor"`          // corresponds to 'flavor' in the 'products' table
	Category       string `json:"category"`        // corresponds to 'category' in the 'products' table
}

func readCSV(filePath string) ([]Product, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Parse the file
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}

	var products []Product
	for _, line := range lines {
		// Here, you should create a Product instance based on the CSV data.
		// The struct fields should correspond to the actual CSV columns.
		// Below, we assume the brand's name is in line[2]. Adjust as necessary.
		products = append(products, Product{
			BrandName: line[2], // assign the brand name from CSV to BrandName field
			// ... assign other fields from the line array
		})
	}
	return products, nil
}

func ensureBrandAndGetID(db *sql.DB, brandName string) (int, error) {
	var brandID int

	// Check if the brand already exists
	err := db.QueryRow("SELECT id FROM brands WHERE name = ?", brandName).Scan(&brandID)
	if err == nil {
		return brandID, nil // Brand found, return the ID
	} else if err != sql.ErrNoRows {
		return 0, err // Some other error occurred
	}

	// Brand doesn't exist, so we create a new one
	result, err := db.Exec("INSERT INTO brands(name) VALUES(?)", brandName)
	if err != nil {
		return 0, err
	}

	newBrandID, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(newBrandID), nil
}
func processProducts(db *sql.DB, products []Product) error {
	// Prepare the statement for updating products
	stmt, err := db.Prepare("UPDATE products SET brand_id = ? WHERE product_id = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, product := range products {
		brandID, err := ensureBrandAndGetID(db, product.BrandName)
		if err != nil {
			return err
		}

		// Update the product's brand reference with the brand ID
		_, err = stmt.Exec(brandID, product.ProductID) // Assuming the product struct has an ID field
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	// Assuming you have an SQLite database file named 'product_brands.db'
	db, err := sql.Open("sqlite3", "./products.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Read products from CSV
	products, err := readCSV("new-vape-juices-gettysburg.csv") // Your CSV path here
	if err != nil {
		log.Fatalf("Failed to read products from CSV: %s", err)
	}

	// Process products and ensure brands
	err = processProducts(db, products)
	if err != nil {
		log.Fatalf("Failed to process products: %s", err)
	}

	fmt.Println("Products have been processed successfully.")
}

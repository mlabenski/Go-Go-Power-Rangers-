package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// ProductRecord represents a product entry with associated categories and the actual product ID.
type ProductRecord struct {
	ProductID  int
	BrandID    int
	Flavor     string
	Categories []string
}

func main() {
	// Open the database connection.
	db, err := sql.Open("sqlite3", "./products.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Query all products.
	rows, err := db.Query(`SELECT product_id, brand_id, flavor, category FROM products`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Map to hold the unique product records. The key is a combination of brandID and flavor.
	productMap := make(map[string]*ProductRecord)

	for rows.Next() {
		var id, brandID int
		var flavor, category string
		if err := rows.Scan(&id, &brandID, &flavor, &category); err != nil {
			log.Fatal(err)
		}

		// Construct the unique key for the product.
		key := fmt.Sprintf("%d-%s", brandID, flavor)

		// If the product combination exists, append the category; otherwise, create a new record.
		if product, exists := productMap[key]; exists {
			product.Categories = append(product.Categories, category)
		} else {
			productMap[key] = &ProductRecord{
				ProductID:  id, // this is the actual product ID that should be referenced
				BrandID:    brandID,
				Flavor:     flavor,
				Categories: []string{category},
			}
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Now, let's insert the unique products into store_products, avoiding conflicts with the UNIQUE constraint.
	for _, product := range productMap {
		// Concatenate categories into a single string.
		joinedCategories := strings.Join(product.Categories, ", ")

		// Insert into store_products, handling the potential UNIQUE constraint conflict.
		// We're using the "INSERT OR IGNORE" strategy here to skip rows that would cause a conflict.
		// This makes sense if we assume that if a product already exists for the store, we don't need to update/change it.
		_, err = db.Exec(`
			INSERT OR IGNORE INTO store_products (store_id, product_id, stock_count, categories) VALUES (?, ?, ?, ?)`,
			1,                 // store_id is assumed to be 1
			product.ProductID, // the actual product ID from the products table
			0,                 // default stock_count
			joinedCategories,  // concatenated categories
		)
		if err != nil {
			log.Printf("Could not insert product with id %d into store_products: %v", product.ProductID, err)
			continue // Skip to the next product on error.
		}
	}

	fmt.Println("Successfully normalized product data and populated store_products with unique products.")
}

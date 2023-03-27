package main

import (
	"net/http"
	"time"
    "math/rand"
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/cors"
)

func generateRandomNumber() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(100)
}

func getNumber(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, generateRandomNumber())
}

func main() {
	router := gin.Default()

	// Add CORS middleware
	config := cors.DefaultConfig()
	config.AllowOrigins = []string{"*"} // allow any origin
	router.Use(cors.New(config))

	router.GET("/random", getNumber)

	router.Run("localhost:8080")
}
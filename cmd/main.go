package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v4"
	"github.com/joho/godotenv"
	"github.com/nats-io/stan.go"
	"github.com/patrickmn/go-cache"
	"log"
	"net/http"
	"os"
)

func main() {

	log.Println("Запуск приложения")

	log.Println("Проверка наличия .env")

	err := godotenv.Load()

	if err != nil {
		fmt.Println("Не найден .env файл")
		os.Exit(1)
	} else {
		log.Println(".env найден")
	}

	// Адрес базы данных
	databaseUrl := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", os.Getenv("NAME"), os.Getenv("PASSWORD"),
		os.Getenv("HOST"), os.Getenv("PORT"), os.Getenv("DATABASE"))

	conn, err := pgx.Connect(context.Background(), databaseUrl)

	log.Println("Проверка соединения с БД")

	if err != nil {
		fmt.Fprintf(os.Stderr, "Соединение не установлено: %v\n", err)
		os.Exit(1)
	} else {
		log.Println("Соединение установлено")
	}

	defer conn.Close(context.Background())

	// Получение всех order_uid из БД и создания кэша
	order_uid := GetOrderUid(conn)
	Cache := cache.New(-1, -1)
	for i := range order_uid {
		data, _ := GetDataByUid(conn, order_uid[i])
		Cache.Set(order_uid[i], data, cache.NoExpiration)
	}
	log.Printf("Восстановление данных из БД в кэш ... записей найдено (%v)\n", len(Cache.Items()))

	//Connect to chanel
	sc, _ := stan.Connect("test-cluster", "simple-pub1")
	defer sc.Close()

	var order OrderInfo

	sc.Subscribe("data", func(m *stan.Msg) {
		err := json.Unmarshal(m.Data, &order)
		if err != nil {
			log.Println("Получены некорректные данные")
			InsertInvalidData(conn, string(m.Data))
		} else {
			log.Println("Получены корректные данные")
			Cache.Set(order.OrderUid, string(m.Data), cache.NoExpiration)
			log.Println("Новые данные записаны в кэш")
			InsertData(conn, order)
		}
	})

	// http сервер который позволяет получить информацию о заказе по order_uid
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	log.Println("Http сервер запущен на http://localhost:8080")
	r.LoadHTMLGlob("templates/*.html")
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{
			"content": "This is an index page...",
		})
	})
	r.POST("/result", func(c *gin.Context) {
		result, _ := Cache.Get(c.PostForm("order_uid"))

		if result == nil {
			c.PureJSON(http.StatusOK, "Записей по такому order_uid не найдено")
		} else {
			c.PureJSON(http.StatusOK, result)
		}
	})
	r.Run(":8080")

}

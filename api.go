/*
Это мой первый проект на GoLang, просьба сильно камнями не бросаться :)
Есть аналогичный функционал на PHP
*/

package main

import (
	"database/sql"
	"fmt"
	"time"
	"log"
	"net/http"
	"encoding/json"
	"io"
	"os"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	_"github.com/go-sql-driver/mysql"
)

// структура для НБРБ курсов
type nbrbRate struct {
	Cur_ID      		int
	Date   				string
	Cur_Abbreviation 	string
	Cur_Scale 			int
	Cur_Name 			string
	Cur_OfficialRate 	float32

}

// структура для базы MySQL
type Rate struct {
	ID				int `json:"id"`
	RateId			int `json:"rate_id"`
	Date			string `json:"date"`
	Abbreviation 	string `json:"abbreviation"`
	Scale     		string `json:"scale"`
	Name   			string `json:"name"`
	OfficialRate 	string `json:"officialRate"`
}

// функция отображения всех курсов в нашем апи, с пагинацией
func getAllRates(w http.ResponseWriter, r *http.Request) {
	var rates []Rate
	var str_limit = ""
	var str_offset = ""
	var total = 0

	limit := r.URL.Query().Get("limit")
	offset := r.URL.Query().Get("offset")

	if limit != "" {
		str_limit = " LIMIT " + limit
		if offset != "" {
			str_offset = " OFFSET " + offset
		} else {
			str_offset = ""
		}
	} else {
		str_limit = ""
		str_offset = ""

	}

	result, err := db_select("SELECT * FROM rates " + str_limit + str_offset);
	 
	for result.Next() { 
		var id int
		var rate_id int
		var date string
		var abbreviation string
		var scale string
		var name string
		var officialRate string
		total++

		err = result.Scan(&id, &rate_id, &date, &abbreviation, &scale, &name, &officialRate)
	
		if err != nil {
			panic(err)
		}
		rates = append(rates, Rate{ID: id, RateId: rate_id, Date: date, Abbreviation: abbreviation, Scale: scale, Name: name, OfficialRate: officialRate}) 
	}
 
	w.Header().Set("Content-Type", "application/json")
	if total > 0 {
		json.NewEncoder(w).Encode(rates)
	} else {
		json.NewEncoder(w).Encode("empty")
	}
}

// функция отображения курсов в нашем апи, с пагинацией
func getRates(w http.ResponseWriter, r *http.Request) {
	var rates []Rate
	var str_limit = ""
	var str_offset = ""
	var total = 0

	params := mux.Vars(r)

	limit := r.URL.Query().Get("limit")
	offset := r.URL.Query().Get("offset")

	if limit != "" {
		str_limit = " LIMIT " + limit
		if offset != "" {
			str_offset = " OFFSET " + offset
		} else {
			str_offset = ""
		}
	} else {
		str_limit = ""
		str_offset = ""
	}
	

	result, err := db_select("SELECT * FROM rates  WHERE date >= '" + params["date"] + "T00:00:00' AND date <= '" + params["date"] + "T23:59:59' " + str_limit + str_offset);
	 
	for result.Next() {
		var id int
		var rate_id int
		var date string
		var abbreviation string
		var scale string
		var name string
		var officialRate string
		total++

		err = result.Scan(&id, &rate_id, &date, &abbreviation, &scale, &name, &officialRate)
	
		if err != nil {
			panic(err)
		}
		rates = append(rates, Rate{ID: id, RateId: rate_id, Date: date, Abbreviation: abbreviation, Scale: scale, Name: name, OfficialRate: officialRate}) 
	}
 
	w.Header().Set("Content-Type", "application/json")
	if total > 0 {
		json.NewEncoder(w).Encode(rates)
	} else {
		json.NewEncoder(w).Encode("empty")
	}

}

func setRates(w http.ResponseWriter, r *http.Request){
	importRates()
}

// функция импорта курсов с апи НБРБ
func importRates(){
	var rates []nbrbRate

	request, err := http.NewRequest("GET", "https://api.nbrb.by/exrates/rates?periodicity=0", nil)
	res, err := http.DefaultClient.Do(request)
	if err != nil {
		panic(err)
	}

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	Data := []byte(resBody)
	err = json.Unmarshal(Data, &rates)
 
	if err != nil {
		panic(err)
	}

	index := 0
	string_inserts := ""
	for i := range rates {
		index++;
		if string_inserts != "" {
			string_inserts += ",";
		}
		string_inserts += fmt.Sprintf("('%d','%s','%s','%d','%s','%f')", rates[i].Cur_ID,rates[i].Date,rates[i].Cur_Abbreviation,rates[i].Cur_Scale,rates[i].Cur_Name,rates[i].Cur_OfficialRate)
		if index == 10 {
			if string_inserts !="" {
				sql := "INSERT INTO `rates`( `rate_id`,`date`, `abbreviation`, `scale`, `name`, `officialRate`) VALUES " + string_inserts + " ON DUPLICATE KEY UPDATE `scale`=VALUES(`scale`),`officialRate`=VALUES(`officialRate`)";
				result, err := db_insert(sql)
				if err != nil {
					panic(err)
				}
				fmt.Println(result)

			}
			index = 0
		}
	}
	if string_inserts !="" {
		sql := "INSERT INTO `rates`( `rate_id`,`date`, `abbreviation`, `scale`, `name`, `officialRate`) VALUES " + string_inserts + " ON DUPLICATE KEY UPDATE `scale`=VALUES(`scale`),`officialRate`=VALUES(`officialRate`)";
		result, err := db_insert(sql)
		if err != nil {
			panic(err)
		}
		fmt.Println(result)

	}
}

func main() {
	// "крон" в 00:00:00
	err := callAt(0, 0, 0, importRates)
	if err != nil {
		panic(err)
	}

	// маршрутизация
	r := mux.NewRouter()
	r.HandleFunc("/api/rates", getAllRates).Methods("GET")
	r.HandleFunc("/api/rates/{date}", getRates).Methods("GET")
	r.HandleFunc("/importRates", setRates).Methods("GET")			//можно дернуть импорт вручную
	
	log.Fatal(http.ListenAndServe(":8000", r))
}

// запрос в базу на селект
func db_select(query string) (*sql.Rows, error){
	db := connect_db()

	result, err := db.Query(query)
	 
	if err != nil {
		panic(err)
	}

	defer db.Close()

	return result, err
}

// запрос в базу на инсерт
func db_insert(query string)(int64, error){
	db := connect_db()

	result, err := db.Exec(query)
	if err != nil {
		panic(err)
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		panic(err)
	}

	return lastInsertID, err
}


// Вызов функции в нужное время
func callAt(hour, min, sec int, f func()) error {
	loc, err := time.LoadLocation("Local")
	if err != nil {
		return err
	}

	// Вычисляем время первого запуска.
	now := time.Now().Local()
	firstCallTime := time.Date(
		now.Year(), now.Month(), now.Day(), hour, min, sec, 0, loc)
	if firstCallTime.Before(now) {
		// Если получилось время раньше текущего, прибавляем сутки.
		firstCallTime = firstCallTime.Add(time.Hour * 24)
	}

	// Вычисляем временной промежуток до запуска.
	duration := firstCallTime.Sub(time.Now().Local())

	go func() {
		time.Sleep(duration)
		for {
			f()
			// Следующий запуск через сутки.
			time.Sleep(time.Hour * 24)
		}
	}()

	return nil
}

// функция подключения к БД
func connect_db()(*sql.DB){
	err := godotenv.Load()
	if err != nil {
	  panic(err)
	}
	dbUsername := os.Getenv("DB_USERNAME")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")

	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUsername, dbPassword, dbHost, dbPort, dbName)

	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
	  panic(err)
	}
	return db
}
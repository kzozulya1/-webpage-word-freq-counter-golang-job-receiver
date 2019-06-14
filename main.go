//Package main.go
package main

import ( 
	logger "app/pkg/loggerutil"
	rbt "app/pkg/amqp"
	m "app/pkg/model"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"os"
)

//Apply CORS
func applyCorsHeaders(w http.ResponseWriter){
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "POST,DELETE,OPTIONS,PUT")
}

//For meet CORS restrictions, for React.js client
func optionsJob(w http.ResponseWriter, r *http.Request) {
	applyCorsHeaders(w)
}


//Create new job API entrypoint -
func createJob(w http.ResponseWriter, r *http.Request) {
	applyCorsHeaders(w)

	var job m.Job
	//Decode posted job
	err := json.NewDecoder(r.Body).Decode(&job)
	if err != nil {
		logger.Log(err.Error(),"error.log") 
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	//Check URL
	if err = checkUrl(job.GetURL());err != nil{
		logger.Log(err.Error(),"error.log")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	//Get Settings
	url,exchange,routeKey := getConfig()

	rabbitClient := rbt.RabbitMqClient{}
	rabbitClient.Init(url, exchange, routeKey)
	defer rabbitClient.Destruct()

	jsonBytes, err := json.Marshal(job)
	if err != nil {
		logger.Log(err.Error(),"error.log")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rabbitClient.PublishJson(jsonBytes)
}

/**
* Vaidate URL
*/
func checkUrl (url string) error {
	client := http.Client{}
	_, err := client.Get(url)
	return err
}

//Main routine
func main() {
	router := mux.NewRouter()
	router.HandleFunc("/job", createJob).Methods("POST")
	router.HandleFunc("/job", optionsJob).Methods("OPTIONS")
	port := os.Getenv("APP_API_PORT")
	http.ListenAndServe(fmt.Sprintf(":%s", port), router)
}

//Get Rabbit MQ client configuration
func getConfig() (string,string,string){
	url := os.Getenv("RMQ_URL")
	if url == "" {
		panic("RabbitMQ URL is not specified")
	}
	exchange := os.Getenv("RMQ_EXCHANGE_NAME") 
	if exchange == ""{
		panic("RabbitMQ exchange name is not specified")
	}
	routeKey := os.Getenv("RMQ_ROUTE_KEY")
	if routeKey == ""{
		panic("RabbitMQ route key is not specified")
	}
    return url, exchange, routeKey
}
